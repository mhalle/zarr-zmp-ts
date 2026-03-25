/**
 * Content decoding — transport-level decompression.
 *
 * Decompresses data based on content_encoding. Uses fflate for
 * deflate/gzip/zlib (covers most cases). Other encodings throw.
 */

import { inflateSync, gunzipSync, unzlibSync } from "fflate";

const decompressors: Record<
  string,
  (data: Uint8Array) => Uint8Array
> = {
  deflate: (data) => inflateSync(data),
  gzip: (data) => gunzipSync(data),
  zlib: (data) => unzlibSync(data),
};

/**
 * Decompress data based on content_encoding.
 * Returns data unchanged if encoding is undefined/empty.
 */
export function decodeContent(
  data: Uint8Array,
  encoding: string | undefined,
): Uint8Array {
  if (!encoding) return data;
  const fn = decompressors[encoding];
  if (!fn) {
    throw new Error(
      `Unsupported content_encoding: "${encoding}". ` +
        `Available: ${Object.keys(decompressors).join(", ")}`,
    );
  }
  return fn(data);
}
