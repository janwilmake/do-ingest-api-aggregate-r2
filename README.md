Problem: carrying out many requests that retrieve large files (over 100MB) isn't easy.

When using SQLite as storage, we are limited to 10GB total per DO, and we'd pay quite a lot. It isn't always ideal for the destination to be a DO.

This worker shows 2 proofs of concept:

1. you can do as many subrequests in a durable object as you want
2. you can stream the results out to a single file in r2 as long as you pass it a predetermined length. to have this fixed length, you can simply ensure to end up with that even if you don't know the actual total length; you can either stop early, or you can append the end of the file with padding.

This worker uses no dependencies and leverages the `FixedLengthStream` web-standard.

# Notes

## Idea: r2 zipper DO:

Problem: i can't possibly get all my r2 items into a single zipfile and store that in an r2...

- I can just query 1000 of them.
- In a queue i can query 1000 each message, but even then, how do I put it into a single zip?

Would it be possible to:

- List all items and queue a message for each 1000 items
- Consume the queue in which I fetch the 1000 items and stream them into a multipart/form-data stream to a DO
- The DO receives a multipart/form-data stream from each queuemessage, and turns it into a single zip by streaming this to R2 and collecting the metadata.
- A final request to the DO to close the zip would send the central directory and close it.

https://claude.ai/chat/dafa225f-7098-4dd0-9cbc-ac5e9e034e6e

Generally:

- input can be kv, r2, sqlite, or any public URLs
- zipping it isn't even required. A giant formdata stream is also fine.

https://developers.cloudflare.com/r2/api/workers/workers-multipart-usage/index.md
https://developers.cloudflare.com/r2/objects/multipart-objects/index.md

Related:

https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-express-one-zone-append-data-object/
https://simonwillison.net/2024/Nov/22/amazon-s3-append-data/

Both multipart uploader and `r2.put` don't allow not knowing the size in advance and passing a readablestream: `✘ [ERROR] Uncaught (in response) TypeError: Provided readable stream must have a known length (request/response body or readable half of FixedLengthStream)`

https://developers.cloudflare.com/workers/runtime-apis/streams/transformstream/#fixedlengthstream

WOW! Fixed length stream works. That's a neat little trick if you know the length. you can round up by adding dashes. This is the only way to stream files into R2!!! Predetermined content-length.
