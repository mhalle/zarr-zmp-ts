import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import type { ManifestEntry, ManifestMetadata } from "./types.js";
import { ZPath } from "./path.js";

/**
 * Hyparquet's AsyncBuffer interface.
 */
export interface AsyncBuffer {
  byteLength: number;
  slice(start: number, end?: number): ArrayBuffer | Promise<ArrayBuffer>;
}

/** Columns to load eagerly (everything except data/data_z). */
const NON_DATA_COLUMNS = [
  "path",
  "size",
  "addressing",
  "content_size",
  "checksum",
  "text",
  "resolve",
  "base_resolve",
  "id",
  "content_type",
  "content_encoding",
  "source",
  "metadata",
];

/**
 * Wraps a ZMP parquet file with in-memory index for O(1) path lookups.
 *
 * All columns except `data`/`data_z` are loaded eagerly. Binary data
 * is read on demand per-row.
 */
export class Manifest {
  private rows: Record<string, unknown>[] = [];
  private pathIndex = new Map<string, number>();
  private idIndex = new Map<string, number>();
  private file: AsyncBuffer;
  private hasDataColumn = false;
  private hasDataZColumn = false;
  private _metadata!: ManifestMetadata;

  private constructor(file: AsyncBuffer) {
    this.file = file;
  }

  static async open(source: string | AsyncBuffer): Promise<Manifest> {
    let file: AsyncBuffer;
    if (typeof source === "string") {
      if (source.startsWith("http://") || source.startsWith("https://")) {
        const { asyncBufferFromUrl } = await import("hyparquet");
        file = await asyncBufferFromUrl({ url: source });
      } else {
        const { asyncBufferFromFile } = await import("hyparquet");
        file = await asyncBufferFromFile(source);
      }
    } else {
      file = source;
    }
    const manifest = new Manifest(file);
    await manifest.init();
    return manifest;
  }

  private async init(): Promise<void> {
    const metadata = await parquetMetadataAsync(this.file);
    const schemaNames = metadata.schema
      .filter((s: { name?: string }) => s.name !== undefined)
      .map((s: { name: string }) => s.name);

    this.hasDataColumn = schemaNames.includes("data");
    this.hasDataZColumn = schemaNames.includes("data_z");

    const availableColumns = NON_DATA_COLUMNS.filter((c) =>
      schemaNames.includes(c),
    );

    this.rows = (await parquetReadObjects({
      file: this.file,
      metadata,
      columns: availableColumns,
      compressors,
    })) as Record<string, unknown>[];

    // Build indexes, normalizing paths to absolute
    for (let i = 0; i < this.rows.length; i++) {
      const raw = this.rows[i].path as string;
      const path = toManifestPath(raw);
      this.rows[i].path = path; // normalize in place
      this.pathIndex.set(path, i);
    }

    for (let i = 0; i < this.rows.length; i++) {
      const id = this.rows[i].id as string | undefined | null;
      if (id != null) {
        this.idIndex.set(id, i);
      }
    }

    this._metadata = parseFileMetadata(metadata.key_value_metadata);
  }

  get metadata(): ManifestMetadata {
    return this._metadata;
  }

  get length(): number {
    return this.rows.length;
  }

  /** Archive-level metadata (from the "" row), or undefined. */
  get archiveMetadata(): Record<string, unknown> | undefined {
    return this.pathMetadata("");
  }

  pathMetadata(path: string): Record<string, unknown> | undefined {
    const p = toManifestPath(path);
    const idx = this.pathIndex.get(p);
    if (idx == null) return undefined;
    return this.rowMetadata(idx);
  }

  private rowMetadata(idx: number): Record<string, unknown> | undefined {
    const raw = this.rows[idx].metadata as string | undefined | null;
    if (raw == null) return undefined;
    try {
      return JSON.parse(raw);
    } catch {
      return undefined;
    }
  }

  getMetadata(opts: {
    path?: string;
    id?: string;
  }): Record<string, unknown> | undefined {
    let idx: number | undefined;
    if (opts.id != null) {
      idx = this.idIndex.get(opts.id);
    } else if (opts.path != null) {
      idx = this.pathIndex.get(toManifestPath(opts.path));
    }
    if (idx == null) return undefined;
    return this.rowMetadata(idx);
  }

  getEntry(path: string): ManifestEntry | undefined {
    const p = toManifestPath(path);
    const idx = this.pathIndex.get(p);
    if (idx == null) return undefined;
    return this.entryAt(idx);
  }

  getEntryById(id: string): ManifestEntry | undefined {
    const idx = this.idIndex.get(id);
    if (idx == null) return undefined;
    return this.entryAt(idx);
  }

  has(path: string): boolean {
    return this.pathIndex.has(toManifestPath(path));
  }

  hasId(id: string): boolean {
    return this.idIndex.has(id);
  }

  async getData(path: string): Promise<Uint8Array | undefined> {
    const p = toManifestPath(path);
    if (!this.hasDataColumn && !this.hasDataZColumn) return undefined;
    const idx = this.pathIndex.get(p);
    if (idx == null) return undefined;

    // Try data column first, then data_z
    for (const col of ["data", "data_z"]) {
      if (col === "data" && !this.hasDataColumn) continue;
      if (col === "data_z" && !this.hasDataZColumn) continue;

      const rows = (await parquetReadObjects({
        file: this.file,
        columns: [col],
        rowStart: idx,
        rowEnd: idx + 1,
        compressors,
        utf8: false,
      })) as Record<string, unknown>[];

      if (rows.length === 0) continue;
      const val = rows[0][col];
      if (val == null) continue;
      if (val instanceof Uint8Array) return val;
      if (val instanceof ArrayBuffer) return new Uint8Array(val);
      if (typeof val === "string") return new TextEncoder().encode(val);
    }
    return undefined;
  }

  isAnnotation(path: string): boolean {
    const p = toManifestPath(path);
    if (p === "") return true;
    const entry = this.getEntry(p);
    if (entry && entry.addressing.includes("F")) return true;
    return false;
  }

  *listPaths(): IterableIterator<string> {
    yield* this.pathIndex.keys();
  }

  *listPrefix(prefix: string): IterableIterator<string> {
    const zprefix = prefix === "" ? ZPath.ROOT : new ZPath(prefix);
    for (const p of this.pathIndex.keys()) {
      if (p === "") continue;
      const zp = new ZPath(p);
      if (zp.isEqualOrChildOf(zprefix)) yield p;
    }
  }

  *listDir(prefix: string): IterableIterator<string> {
    const zprefix = prefix === "" || prefix === "/" ? ZPath.ROOT : new ZPath(prefix);
    const seen = new Set<string>();

    for (const p of this.pathIndex.keys()) {
      if (p === "" || this.isAnnotation(p)) continue;
      const zp = new ZPath(p);
      const child = zp.childNameUnder(zprefix);
      if (child == null) continue;
      const rel = zp.relativeTo(zprefix);
      const isDir = rel.includes("/");
      const entry = isDir ? child + "/" : child;
      if (!seen.has(entry)) {
        seen.add(entry);
        yield entry;
      }
    }
  }

  private entryAt(idx: number): ManifestEntry {
    const row = this.rows[idx];
    const addr = row.addressing as string | string[] | undefined | null;
    // Handle both old list<string> and new string format
    const addressing =
      typeof addr === "string"
        ? addr
        : Array.isArray(addr)
          ? addr.join("")
          : "";

    return {
      path: row.path as string,
      size: toNumber(row.size),
      addressing,
      content_size: toOptionalNumber(row.content_size),
      checksum: row.checksum as string | undefined,
      text: row.text as string | undefined,
      resolve: row.resolve as string | undefined,
      base_resolve: row.base_resolve as string | undefined,
      id: row.id as string | undefined,
      content_type: row.content_type as string | undefined,
      content_encoding: row.content_encoding as string | undefined,
      source: row.source as string | undefined,
      metadata: row.metadata as string | undefined,
    };
  }
}

// --- Helpers ---

function toManifestPath(path: string): string {
  if (path === "") return "";
  return String(new ZPath(path));
}

function toNumber(val: unknown): number {
  if (typeof val === "bigint") return Number(val);
  if (typeof val === "number") return val;
  return 0;
}

function toOptionalNumber(val: unknown): number | undefined {
  if (val == null) return undefined;
  if (typeof val === "bigint") return Number(val);
  if (typeof val === "number") return val;
  return undefined;
}

function parseFileMetadata(
  kvPairs?: { key: string; value?: string }[],
): ManifestMetadata {
  const STRING_KEYS = new Set(["zmp_version", "zarr_format", "retrieval_scheme"]);
  const JSON_KEYS = new Set(["base_resolve"]);
  const result: Record<string, unknown> = {};
  if (kvPairs) {
    for (const { key, value } of kvPairs) {
      if (value == null) continue;
      if (STRING_KEYS.has(key)) {
        result[key] = value; // keep as string
      } else if (JSON_KEYS.has(key)) {
        try { result[key] = JSON.parse(value); } catch { result[key] = value; }
      } else {
        try { result[key] = JSON.parse(value); } catch { result[key] = value; }
      }
    }
  }

  const meta: ManifestMetadata = {
    zmp_version: (result.zmp_version as string) ?? "",
    zarr_format: (result.zarr_format as string) ?? "",
    retrieval_scheme: (result.retrieval_scheme as string) ?? "",
  };

  const extra: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(result)) {
    if (["zmp_version", "zarr_format", "retrieval_scheme", "pandas"].includes(k)) continue;
    extra[k] = v;
  }
  if (Object.keys(extra).length > 0) meta.extra = extra;

  return meta;
}
