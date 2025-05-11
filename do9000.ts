//@ts-check
/// <reference types="@cloudflare/workers-types" />
// src/index.ts
// This version uses FixedLengthStream to ensure known content length for R2 uploads

interface Env {
  COUNTER: DurableObjectNamespace;
  R2_BUCKET: R2Bucket;
}

interface HackerNewsItem {
  id: number;
  title?: string;
  type?: string;
  by?: string;
  time?: number;
  text?: string;
  url?: string;
  score?: number;
  [key: string]: any; // For other possible properties
}

interface CollectionStats {
  bytesCollected: number;
  targetLength: number;
  fileName: string;
  startTime: string | null;
  lastItemTime: string | null;
  runningFor: string | null;
  itemCount: number;
  status: string;
}

interface ItemResponse {
  id: number;
  data: HackerNewsItem;
  collected: string;
  byteSize: number;
}

export class SubrequestCounter {
  private state: DurableObjectState;
  private env: Env;
  private storage: DurableObjectStorage;
  private collectionActive: boolean = false;
  private bytesCollected: number = 0;
  private targetLength: number = 0;
  private itemCount: number = 0;
  private startTime: number | null = null;
  private lastItemTime: number | null = null;
  private fileName: string = "";
  private latestItems: ItemResponse[] = [];
  private maxLatestItems: number = 100;
  private status: string = "idle";

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.storage = state.storage;

    // Initialize state from storage
    this.state.blockConcurrencyWhile(async () => {
      this.bytesCollected = (await this.storage.get("bytesCollected")) || 0;
      this.targetLength = (await this.storage.get("targetLength")) || 0;
      this.itemCount = (await this.storage.get("itemCount")) || 0;
      this.startTime = (await this.storage.get("startTime")) || null;
      this.lastItemTime = (await this.storage.get("lastItemTime")) || null;
      this.fileName = (await this.storage.get("fileName")) || "";
      this.latestItems = (await this.storage.get("latestItems")) || [];
      this.status = (await this.storage.get("status")) || "idle";
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname.slice(1);

    // Start collection process
    if (path === "start") {
      if (this.collectionActive) {
        return new Response(
          JSON.stringify({
            message: "Collection is already in progress",
          }),
          {
            headers: { "Content-Type": "application/json" },
          },
        );
      }

      const length = parseInt(url.searchParams.get("length") || "10000", 10);
      if (isNaN(length) || length <= 0 || length > 100000000) {
        return new Response(
          JSON.stringify({
            error:
              "Invalid length parameter. Must be a positive number up to 100MB.",
          }),
          {
            status: 400,
            headers: { "Content-Type": "application/json" },
          },
        );
      }

      // Generate a unique filename for this collection
      this.fileName = `hn-items-${Date.now()}.jsonl`;
      this.targetLength = length;
      this.startTime = Date.now();
      this.bytesCollected = 0;
      this.itemCount = 0;
      this.status = "collecting";

      // Save initial state
      await this.storage.put("fileName", this.fileName);
      await this.storage.put("targetLength", this.targetLength);
      await this.storage.put("startTime", this.startTime);
      await this.storage.put("bytesCollected", this.bytesCollected);
      await this.storage.put("itemCount", this.itemCount);
      await this.storage.put("status", this.status);

      // Start the collection without waiting for it to complete
      this.collectData(length);

      return new Response(
        JSON.stringify({
          message: `Started collecting data up to ${length} bytes, storing to R2 as ${this.fileName}`,
        }),
        {
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    // Get status and stats
    if (path === "status") {
      const stats = await this.getStats();
      return new Response(JSON.stringify(stats), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // Get the latest items
    if (path === "items") {
      const limit = parseInt(url.searchParams.get("limit") || "10", 10);
      const items = await this.getLatestItems(limit);
      return new Response(JSON.stringify(items), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // Reset the collection
    if (path === "reset") {
      await this.resetCollection();
      return new Response(
        JSON.stringify({
          message: "Collection reset successfully",
        }),
        {
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(
      JSON.stringify({
        error: "Unknown action",
        availableActions: [
          "/start?length=N - Start collecting data up to N bytes from Hacker News API",
          "/status - Get current collection status and stats",
          "/items?limit=N - Get the latest N items collected",
          "/reset - Reset the collection",
        ],
      }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  async collectData(targetLength: number): Promise<void> {
    this.collectionActive = true;
    const encoder = new TextEncoder();

    try {
      // Prepare buffer for collected data
      let buffer = "";
      let currentSize = 0;

      // Continue fetching until we reach target length
      while (currentSize < targetLength && this.collectionActive) {
        // Generate a random HN story ID
        const id = 1 + Math.floor(Math.random() * 35000000);

        // Fetch the item
        const item = await this.fetchItem(id);

        if (item) {
          const timestamp = Date.now();
          const enrichedItem = {
            ...item,
            _collected: timestamp,
          };

          // Convert to JSONL format
          const jsonLine = JSON.stringify(enrichedItem) + "\n";
          const itemSize = encoder.encode(jsonLine).length;

          // Check if adding this item would exceed the target length
          if (currentSize + itemSize > targetLength) {
            // Fill remaining space with dashes
            const remainingBytes = targetLength - currentSize;
            if (remainingBytes > 0) {
              const padding = "-".repeat(remainingBytes - 1) + "\n";
              buffer += padding;
              currentSize += encoder.encode(padding).length;
            }
            break;
          }

          // Add item to buffer
          buffer += jsonLine;
          currentSize += itemSize;

          // Update stats
          this.itemCount++;
          this.bytesCollected = currentSize;
          this.lastItemTime = timestamp;

          // Add to latest items cache
          const itemResponse: ItemResponse = {
            id: item.id,
            data: item,
            collected: new Date(timestamp).toISOString(),
            byteSize: itemSize,
          };

          this.latestItems.unshift(itemResponse);

          // Trim latest items if needed
          if (this.latestItems.length > this.maxLatestItems) {
            this.latestItems = this.latestItems.slice(0, this.maxLatestItems);
          }

          // Update storage with latest stats
          await this.storage.put("bytesCollected", this.bytesCollected);
          await this.storage.put("itemCount", this.itemCount);
          await this.storage.put("lastItemTime", this.lastItemTime);
          await this.storage.put("latestItems", this.latestItems);
        }
      }

      // Once we have the exact content with known length, upload using FixedLengthStream
      await this.uploadWithFixedLength(buffer, currentSize);

      this.status = "completed";
      await this.storage.put("status", this.status);
    } catch (error) {
      console.error("Error during collection:", error);
      this.status = "error";
      await this.storage.put("status", this.status);
    } finally {
      this.collectionActive = false;
    }
  }

  async uploadWithFixedLength(content: string, length: number): Promise<void> {
    try {
      // Create a FixedLengthStream with the exact content length
      const { readable, writable } = new FixedLengthStream(length);

      // Create a writer for the stream
      const writer = writable.getWriter();

      // Start the upload to R2 using the readable end of the stream
      const uploadPromise = this.env.R2_BUCKET.put(this.fileName, readable, {
        contentType: "application/jsonl",
      });

      // Encode and write the content to the stream
      const encoder = new TextEncoder();
      const data = encoder.encode(content);
      await writer.write(data);
      await writer.close();

      // Wait for the upload to complete
      await uploadPromise;

      console.log(`Successfully uploaded ${length} bytes to ${this.fileName}`);
    } catch (error) {
      console.error("Error uploading to R2:", error);
      throw error;
    }
  }

  async fetchItem(id: number): Promise<HackerNewsItem | null> {
    try {
      const response = await fetch(
        `https://hacker-news.firebaseio.com/v0/item/${id}.json`,
      );
      const data: HackerNewsItem = await response.json();

      // Only return items that exist
      if (data && data.id) {
        return data;
      }
      return null;
    } catch (error) {
      console.error(`Error fetching item ${id}:`, error);
      return null;
    }
  }

  async getStats(): Promise<CollectionStats> {
    return {
      bytesCollected: this.bytesCollected,
      targetLength: this.targetLength,
      fileName: this.fileName,
      startTime: this.startTime ? new Date(this.startTime).toISOString() : null,
      lastItemTime: this.lastItemTime
        ? new Date(this.lastItemTime).toISOString()
        : null,
      runningFor: this.startTime
        ? Math.floor((Date.now() - (this.startTime || 0)) / 1000) + " seconds"
        : null,
      itemCount: this.itemCount,
      status: this.status,
    };
  }

  async getLatestItems(limit: number): Promise<ItemResponse[]> {
    return this.latestItems.slice(0, limit);
  }

  async resetCollection(): Promise<void> {
    // Stop any active collection
    this.collectionActive = false;

    // Reset stats
    this.bytesCollected = 0;
    this.targetLength = 0;
    this.itemCount = 0;
    this.startTime = null;
    this.lastItemTime = null;
    this.fileName = "";
    this.latestItems = [];
    this.status = "idle";

    // Update storage
    await this.storage.put("bytesCollected", this.bytesCollected);
    await this.storage.put("targetLength", this.targetLength);
    await this.storage.put("itemCount", this.itemCount);
    await this.storage.put("startTime", this.startTime);
    await this.storage.put("lastItemTime", this.lastItemTime);
    await this.storage.put("fileName", this.fileName);
    await this.storage.put("latestItems", this.latestItems);
    await this.storage.put("status", this.status);
  }
}

// Worker to route requests to the Durable Object
export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);

    // Create a unique ID for our Durable Object
    const doId = env.COUNTER.idFromName("hn-data-collector");
    const durableObject = env.COUNTER.get(doId);

    // Forward the request to the Durable Object
    return durableObject.fetch(request);
  },
};
