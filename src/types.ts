/**
 * Addressing flags indicating how an entry's content can be resolved.
 */
export const Addressing = {
  TEXT: "T",
  DATA: "D",
  DATA_Z: "Z",
  RESOLVE: "R",
  LINK: "L",
  MOUNT: "M",
  FOLDER: "F",
  INDEX: "I",
} as const;

export type AddressingFlag = (typeof Addressing)[keyof typeof Addressing];

/**
 * Transport-level compression encoding.
 */
export const ContentEncoding = {
  DEFLATE: "deflate",
  GZIP: "gzip",
  ZLIB: "zlib",
  BZ2: "bz2",
  LZMA: "lzma",
  ZSTD: "zstd",
  LZ4: "lz4",
  BR: "br",
} as const;

export type ContentEncodingValue =
  (typeof ContentEncoding)[keyof typeof ContentEncoding];

/**
 * File-level metadata from the parquet key-value pairs.
 */
export interface ManifestMetadata {
  zmp_version: string;
  zarr_format: string;
  retrieval_scheme: string;
  extra?: Record<string, unknown>;
}

/**
 * A single entry from the manifest.
 *
 * - `size`: logical (decompressed) byte count
 * - `content_size`: stored (compressed) byte count
 * - `checksum`: git-sha1 hash
 * - `content_encoding`: transport compression (deflate, zstd, etc.)
 */
export interface ManifestEntry {
  path: string;
  size: number;
  addressing: string;
  content_size?: number;
  checksum?: string;
  text?: string;
  resolve?: string; // JSON string
  base_resolve?: string; // JSON string
  id?: string;
  content_type?: string;
  content_encoding?: string;
  source?: string;
  metadata?: string; // JSON string
}

/**
 * Parsed resolve dict — keyed by scheme.
 */
export type ResolveDict = Record<string, Record<string, unknown>>;

/**
 * Protocol for scheme-specific content resolvers.
 */
export interface Resolver {
  resolve(
    params: Record<string, unknown>,
    bases?: Record<string, unknown>[],
  ): Promise<Uint8Array | undefined>;
}
