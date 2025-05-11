//@ts-check
/// <reference types="@cloudflare/workers-types" />
// wrangler.toml configuration remains the same
/*
name = "r2-batch-uploader"
main = "src/index.ts"
compatibility_date = "2025-04-25"

[[r2_buckets]]
binding = "SOURCE_BUCKET"
bucket_name = "your-source-bucket"
preview_bucket_name = "your-source-bucket"

[[r2_buckets]]
binding = "DESTINATION_BUCKET" 
bucket_name = "your-destination-bucket"
preview_bucket_name = "your-destination-bucket"

[[kv_namespaces]]
binding = "UPLOADER_STATE"
id = "your-kv-namespace-id"
preview_id = "your-kv-namespace-id"

[[queues.producers]]
queue = "file-batches"
binding = "FILE_QUEUE"
*/

// src/index.ts
export interface Env {
  SOURCE_BUCKET: R2Bucket;
  DESTINATION_BUCKET: R2Bucket;
  UPLOADER_STATE: KVNamespace;
  FILE_QUEUE: Queue;
}

// Constants
const MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB, R2 minimum part size
const MAX_PART_SIZE = 5 * 1024 * 1024 * 1024; // 5 GiB, R2 maximum part size
const MAX_BATCH_FILES = 1000; // Maximum files to process in one batch
const MAX_PARTS = 10000; // R2 maximum parts limit
const CRLF = "\r\n";

export interface JobState {
  id: string;
  targetKey: string;
  prefix: string;
  uploadId: string;
  boundary: string;
  totalFiles: number;
  processedFiles: number;
  inProgress: boolean;
  isComplete: boolean;
  error?: string;
  parts: Array<{
    PartNumber: number;
    ETag: string;
    size: number;
  }>;
  currentBatchSize: number; // Track the current batch size for streaming
  currentPartNumber: number; // Track the current part number
}

interface FileMetadata {
  key: string;
  size: number;
  contentType?: string;
  etag?: string;
  httpMetadata?: Record<string, string>;
}

// Main worker entrypoint
export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    const action = url.searchParams.get("action");

    // API for starting a new job
    if (path === "/job" && action === "start" && request.method === "POST") {
      return handleStartJob(request, env, ctx);
    }

    // API for getting job status
    if (path === "/job" && action === "status" && request.method === "GET") {
      return handleJobStatus(request, env, ctx);
    }

    // API for manually completing a job
    if (path === "/job" && action === "complete" && request.method === "POST") {
      return handleCompleteJob(request, env, ctx);
    }

    return new Response("Not found or method not allowed", {
      status: 404,
      headers: { Allow: "GET, POST" },
    });
  },

  // Queue consumer for batch processing
  async queue(
    batch: MessageBatch<any>,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    for (const message of batch.messages) {
      const payload = message.body;

      if (payload.type === "listBatch") {
        await processListBatch(payload, env, ctx);
      } else if (payload.type === "processBatch") {
        await processFileBatch(payload, env, ctx);
      } else if (payload.type === "finalizeMultipart") {
        await finalizeMultipartFormData(payload, env, ctx);
      }
    }
  },
};

// Handle starting a new job
async function handleStartJob(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  try {
    const { targetKey, prefix = "" } = await request.json();

    if (!targetKey) {
      return new Response("Missing targetKey parameter", { status: 400 });
    }

    // Create a unique ID for this job
    const jobId = crypto.randomUUID();
    // Generate a unique boundary for multipart/form-data
    const boundary = `----WebKitFormBoundary${crypto
      .randomUUID()
      .replace(/-/g, "")}`;

    // Create a multipart upload in R2
    const multipartUpload = await env.DESTINATION_BUCKET.createMultipartUpload(
      targetKey,
    );

    // Initialize job state
    const jobState: JobState = {
      id: jobId,
      targetKey,
      prefix,
      uploadId: multipartUpload.uploadId,
      boundary,
      totalFiles: 0,
      processedFiles: 0,
      inProgress: true,
      isComplete: false,
      parts: [],
      currentBatchSize: 0,
      currentPartNumber: 1,
    };

    // Save initial job state
    await env.UPLOADER_STATE.put(`job:${jobId}`, JSON.stringify(jobState));

    // Queue the first listing operation
    await env.FILE_QUEUE.send({
      type: "listBatch",
      jobId,
      prefix,
      continuationToken: null,
    });

    return new Response(
      JSON.stringify({
        status: "started",
        jobId,
        uploadId: multipartUpload.uploadId,
        boundary,
        message:
          "Upload job started. Check status with ?action=status&jobId=" + jobId,
      }),
      {
        headers: { "Content-Type": "application/json" },
      },
    );
  } catch (error) {
    return new Response(`Error starting job: ${error.message}`, {
      status: 500,
    });
  }
}

// Handle job status request
async function handleJobStatus(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const url = new URL(request.url);
  const jobId = url.searchParams.get("jobId");

  if (!jobId) {
    return new Response("Missing jobId parameter", { status: 400 });
  }

  const jobState = await getJobState(env, jobId);
  if (!jobState) {
    return new Response(`Job ${jobId} not found`, { status: 404 });
  }

  return new Response(
    JSON.stringify({
      jobId: jobState.id,
      targetKey: jobState.targetKey,
      uploadId: jobState.uploadId,
      boundary: jobState.boundary,
      inProgress: jobState.inProgress,
      isComplete: jobState.isComplete,
      error: jobState.error,
      totalFiles: jobState.totalFiles,
      processedFiles: jobState.processedFiles,
      parts: jobState.parts.length,
      currentBatchSize: jobState.currentBatchSize,
      currentPartNumber: jobState.currentPartNumber,
      percentComplete:
        jobState.totalFiles > 0
          ? Math.round((jobState.processedFiles / jobState.totalFiles) * 100)
          : 0,
    }),
    {
      headers: { "Content-Type": "application/json" },
    },
  );
}

// Handle completing a job
async function handleCompleteJob(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  try {
    const url = new URL(request.url);
    const jobId = url.searchParams.get("jobId");

    if (!jobId) {
      return new Response("Missing jobId parameter", { status: 400 });
    }

    const jobState = await getJobState(env, jobId);
    if (!jobState) {
      return new Response(`Job ${jobId} not found`, { status: 404 });
    }

    if (jobState.isComplete) {
      return new Response(`Job ${jobId} is already complete`, { status: 400 });
    }

    // If the job is still processing files, don't allow completion yet
    if (jobState.inProgress) {
      return new Response(
        `Job ${jobId} is still processing. Wait until inProgress is false.`,
        { status: 400 },
      );
    }

    // If there's an active part that hasn't been committed yet, commit it
    if (jobState.currentBatchSize > 0) {
      await uploadFinalStreamingPart(env, jobState);
    }

    // Queue the final part that will close the multipart/form-data
    await env.FILE_QUEUE.send({
      type: "finalizeMultipart",
      jobId,
    });

    return new Response(
      JSON.stringify({
        status: "finalizing",
        jobId,
        message: "Job is being finalized. Check status to confirm completion.",
      }),
      {
        headers: { "Content-Type": "application/json" },
      },
    );
  } catch (error) {
    return new Response(`Error completing job: ${error.message}`, {
      status: 500,
    });
  }
}

// Process a batch of object listings and queue the next batch
async function processListBatch(
  payload: any,
  env: Env,
  ctx: ExecutionContext,
): Promise<void> {
  const { jobId, prefix, continuationToken } = payload;

  // Get the current job state
  const jobState = await getJobState(env, jobId);
  if (!jobState || jobState.isComplete) {
    console.log(`Job ${jobId} not found or already complete`);
    return;
  }

  try {
    // List objects from R2
    const objects = await env.SOURCE_BUCKET.list({
      prefix,
      delimiter: "",
      cursor: continuationToken,
      limit: MAX_BATCH_FILES,
    });

    // Collect file metadata for more efficient processing
    const fileMetadata: FileMetadata[] = objects.objects.map((obj) => ({
      key: obj.key,
      size: obj.size,
      etag: obj.etag,
      httpMetadata: obj.httpMetadata as Record<string, string>,
    }));

    // Update total files count
    const newTotalFiles = jobState.totalFiles + fileMetadata.length;
    await updateJobState(env, jobId, {
      totalFiles: newTotalFiles,
    });

    // If we have objects, queue them for processing
    if (fileMetadata.length > 0) {
      await env.FILE_QUEUE.send({
        type: "processBatch",
        jobId,
        fileMetadata,
      });
    }

    // If there are more objects, queue the next listing operation
    if (objects.truncated) {
      await env.FILE_QUEUE.send({
        type: "listBatch",
        jobId,
        prefix,
        continuationToken: objects.cursor,
      });
    } else if (fileMetadata.length === 0) {
      // No more files to process and none were processed in this batch
      // Mark job as not in progress anymore if all files have been processed
      const latestJobState = await getJobState(env, jobId);
      if (
        latestJobState &&
        latestJobState.processedFiles >= latestJobState.totalFiles
      ) {
        // If there's an active part that hasn't been committed yet, commit it
        if (latestJobState.currentBatchSize > 0) {
          await uploadFinalStreamingPart(env, latestJobState);
        }

        await updateJobState(env, jobId, {
          inProgress: false,
        });
      }
    }
  } catch (error) {
    console.error(`Error processing list batch for job ${jobId}:`, error);
    await updateJobState(env, jobId, {
      error: `Error listing files: ${error.message}`,
      inProgress: false,
    });
  }
}

// Process a batch of files using streaming
async function processFileBatch(
  payload: any,
  env: Env,
  ctx: ExecutionContext,
): Promise<void> {
  const { jobId, fileMetadata } = payload;

  // Get the current job state
  const jobState = await getJobState(env, jobId);
  if (!jobState || jobState.isComplete) {
    console.log(`Job ${jobId} not found or already complete`);
    return;
  }

  try {
    // Check if we've reached the part limit
    if (jobState.parts.length >= MAX_PARTS) {
      await updateJobState(env, jobId, {
        error: `Maximum number of parts (${MAX_PARTS}) reached`,
        inProgress: false,
      });
      return;
    }

    let filesProcessed = 0;
    let filesSize = 0;
    const boundary = jobState.boundary;

    // Initialize the multipart upload for streaming
    let multipartUpload = env.DESTINATION_BUCKET.resumeMultipartUpload(
      jobState.targetKey,
      jobState.uploadId,
    );

    // Create a streaming writer to R2
    let uploadController: ReadableStreamDefaultController<Uint8Array> | null =
      null;
    let uploadComplete: Promise<R2UploadedPart> | null = null;
    let streamStarted = false;

    // Process each file and stream it to R2
    for (const file of fileMetadata) {
      try {
        const object = await env.SOURCE_BUCKET.get(file.key);
        if (!object) continue;

        // Extract metadata from the file
        const contentType =
          object.httpMetadata?.contentType || "application/octet-stream";
        const fileName = file.key.split("/").pop() || file.key;
        const fileSize = object.size;

        // Check if adding this file would exceed the maximum part size
        const estimatedHeaderSize = 500; // Estimate headers size (varies based on metadata)
        const fileTotalSize = fileSize + estimatedHeaderSize + CRLF.length * 2;

        // If current batch size + this file would exceed max part size, or we haven't started a stream yet
        if (
          jobState.currentBatchSize + fileTotalSize > MAX_PART_SIZE ||
          !streamStarted
        ) {
          // If we have an active stream, finalize it
          if (streamStarted && uploadController) {
            uploadController.close();

            // Wait for the upload to complete
            if (uploadComplete) {
              const uploadedPart = await uploadComplete;

              // Add to parts list with size information
              jobState.parts.push({
                PartNumber: jobState.currentPartNumber,
                ETag: uploadedPart.etag,
                size: jobState.currentBatchSize,
              });

              // Update part number and reset batch size
              jobState.currentPartNumber++;
              jobState.currentBatchSize = 0;

              // Update job state with new part info
              await updateJobState(env, jobState.id, {
                parts: jobState.parts,
                currentPartNumber: jobState.currentPartNumber,
                currentBatchSize: 0,
              });

              console.log(
                `Uploaded part ${jobState.currentPartNumber - 1} for job ${
                  jobState.id
                }, size: ${uploadedPart.size} bytes`,
              );
            }
          }

          // Start a new stream for the next part
          const { readable, writable } = new TransformStream<Uint8Array>();
          uploadComplete = multipartUpload.uploadPart(
            jobState.currentPartNumber,
            readable,
          );

          const writer = writable.getWriter();
          streamStarted = true;

          // Create a controller for the upload stream
          const readableStream = new ReadableStream({
            start(controller) {
              uploadController = controller;
            },
          });
        }

        // Prepare the multipart/form-data headers for this file
        const headers = [
          `--${boundary}`,
          `Content-Disposition: form-data; name="file"; filename="${fileName}"`,
          `Content-Type: ${contentType}`,
        ];

        // Add custom metadata if available
        if (object.httpMetadata) {
          for (const [key, value] of Object.entries(object.httpMetadata)) {
            if (key !== "contentType" && key !== "contentDisposition") {
              headers.push(`X-Metadata-${key}: ${value}`);
            }
          }
        }

        // Add an empty line after headers
        headers.push("", "");

        // Convert headers to Uint8Array
        const headersText = headers.join(CRLF);
        const headersBuffer = new TextEncoder().encode(headersText);

        // Stream the headers
        if (uploadController) {
          uploadController.enqueue(headersBuffer);
          jobState.currentBatchSize += headersBuffer.length;
        }

        // Stream the file data
        if (object.body && uploadController) {
          const reader = object.body.getReader();

          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            if (value) {
              uploadController.enqueue(value);
              jobState.currentBatchSize += value.length;
            }
          }
        }

        // Add a trailing CRLF after file data
        const trailingCRLF = new TextEncoder().encode(CRLF);
        if (uploadController) {
          uploadController.enqueue(trailingCRLF);
          jobState.currentBatchSize += trailingCRLF.length;
        }

        // Update job state with current batch size
        await updateJobState(env, jobState.id, {
          currentBatchSize: jobState.currentBatchSize,
        });

        filesProcessed++;
        filesSize += fileSize;
      } catch (error) {
        console.error(`Error processing file ${file.key}:`, error);
        // Continue with other files if one fails
      }
    }

    // Update processed files count
    if (filesProcessed > 0) {
      const updatedJob = await getJobState(env, jobId);
      if (updatedJob) {
        const newProcessedCount = updatedJob.processedFiles + filesProcessed;
        const updates: Partial<JobState> = {
          processedFiles: newProcessedCount,
        };

        // If all files are processed, mark the job as not in progress
        if (newProcessedCount >= updatedJob.totalFiles) {
          // Only mark as not in progress, actual finalizing happens in handleCompleteJob
          updates.inProgress = false;
        }

        await updateJobState(env, jobId, updates);
      }
    }
  } catch (error) {
    console.error(`Error processing batch for job ${jobId}:`, error);
    await updateJobState(env, jobId, {
      error: `Error processing files: ${error.message}`,
      inProgress: false,
    });
  }
}

// Upload the final streaming part when the batch is complete
async function uploadFinalStreamingPart(
  env: Env,
  jobState: JobState,
): Promise<void> {
  try {
    if (jobState.currentBatchSize <= 0) {
      return; // Nothing to upload
    }

    // Resume the multipart upload
    const multipartUpload = env.DESTINATION_BUCKET.resumeMultipartUpload(
      jobState.targetKey,
      jobState.uploadId,
    );

    // Create a stream for the final part
    const { readable, writable } = new TransformStream<Uint8Array>();
    const uploadComplete = multipartUpload.uploadPart(
      jobState.currentPartNumber,
      readable,
    );

    const writer = writable.getWriter();

    // Create an empty buffer to finalize the current part
    const emptyBuffer = new Uint8Array(0);
    await writer.write(emptyBuffer);
    await writer.close();

    // Wait for the upload to complete
    const uploadedPart = await uploadComplete;

    // Add to parts list with size information
    jobState.parts.push({
      PartNumber: jobState.currentPartNumber,
      ETag: uploadedPart.etag,
      size: jobState.currentBatchSize,
    });

    // Update part number and reset batch size
    jobState.currentPartNumber++;
    jobState.currentBatchSize = 0;

    // Update job state with new part info
    await updateJobState(env, jobState.id, {
      parts: jobState.parts,
      currentPartNumber: jobState.currentPartNumber,
      currentBatchSize: 0,
    });

    console.log(
      `Uploaded final streaming part ${
        jobState.currentPartNumber - 1
      } for job ${jobState.id}, size: ${uploadedPart.size} bytes`,
    );
  } catch (error) {
    console.error(
      `Error uploading final streaming part for job ${jobState.id}:`,
      error,
    );
    throw error;
  }
}

// Finalize the multipart/form-data with closing boundary
async function finalizeMultipartFormData(
  payload: any,
  env: Env,
  ctx: ExecutionContext,
): Promise<void> {
  const { jobId } = payload;

  // Get the current job state
  const jobState = await getJobState(env, jobId);
  if (!jobState || jobState.isComplete) {
    console.log(`Job ${jobId} not found or already complete`);
    return;
  }

  try {
    // Create the closing boundary
    const closingBoundary = new TextEncoder().encode(
      `--${jobState.boundary}--${CRLF}`,
    );

    // Resume the multipart upload
    const multipartUpload = env.DESTINATION_BUCKET.resumeMultipartUpload(
      jobState.targetKey,
      jobState.uploadId,
    );

    // Create a stream for the final part with closing boundary
    const { readable, writable } = new TransformStream<Uint8Array>();
    const uploadComplete = multipartUpload.uploadPart(
      jobState.currentPartNumber,
      readable,
    );

    const writer = writable.getWriter();
    await writer.write(closingBoundary);
    await writer.close();

    // Wait for the upload to complete
    const uploadedPart = await uploadComplete;

    // Add to parts list with size information
    jobState.parts.push({
      PartNumber: jobState.currentPartNumber,
      ETag: uploadedPart.etag,
      size: closingBoundary.length,
    });

    // Complete the multipart upload
    const parts = jobState.parts.map((part) => ({
      partNumber: part.PartNumber,
      etag: part.ETag,
    }));

    const object = await multipartUpload.complete(parts);

    // Mark job as complete
    await updateJobState(env, jobState.id, {
      isComplete: true,
      inProgress: false,
      parts: jobState.parts,
    });

    console.log(
      `Completed multipart upload for job ${jobState.id}, file: ${jobState.targetKey}, etag: ${object.httpEtag}`,
    );
  } catch (error) {
    console.error(
      `Error finalizing multipart form data for job ${jobId}:`,
      error,
    );
    await updateJobState(env, jobId, {
      error: `Error finalizing multipart: ${error.message}`,
      inProgress: false,
    });
  }
}

// Helper to get job state from KV
async function getJobState(env: Env, jobId: string): Promise<JobState | null> {
  const jobData = await env.UPLOADER_STATE.get(`job:${jobId}`);
  if (!jobData) {
    return null;
  }

  return JSON.parse(jobData) as JobState;
}

// Helper to update job state in KV
async function updateJobState(
  env: Env,
  jobId: string,
  updates: Partial<JobState>,
): Promise<JobState> {
  const currentJob = await getJobState(env, jobId);
  if (!currentJob) {
    throw new Error(`Job ${jobId} not found`);
  }

  const updatedJob = { ...currentJob, ...updates };
  await env.UPLOADER_STATE.put(`job:${jobId}`, JSON.stringify(updatedJob));

  return updatedJob;
}
