/**
 * Read-only Zarr v3 store backed by a ZMP manifest.
 */

import { Manifest } from "./manifest.js";
import { ZPath } from "./path.js";
import { Addressing } from "./types.js";
import { HttpResolver } from "./resolver.js";
import { resolveEntry, getFileBaseResolve } from "./resolve.js";
import type { ManifestEntry, Resolver } from "./types.js";

/**
 * zarrita's structural interface for an async readable store.
 */
export interface AsyncReadable {
  get(key: string): Promise<Uint8Array | undefined>;
}

export type MountOpener = (entry: ManifestEntry) => Promise<ZMPStore>;

export interface ZMPStoreOptions {
  resolvers?: Record<string, Resolver>;
  mountOpener?: MountOpener;
  baseResolve?: Record<string, unknown>[];
}

/**
 * Read-only Zarr v3 store backed by a ZMP manifest parquet file.
 */
export class ZMPStore implements AsyncReadable {
  private manifest: Manifest;
  private resolvers: Record<string, Resolver>;
  private mountOpenerFn: MountOpener;
  private baseResolve: Record<string, unknown>[] | undefined;
  private mounts = new Map<string, ZMPStore>();
  private mountPrefixes: ZPath[] = [];
  private linkPrefixes = new Map<ZPath, ZPath>();
  private chunkSizes = new Map<string, number | null>();

  private constructor(
    manifest: Manifest,
    resolvers?: Record<string, Resolver>,
    mountOpener?: MountOpener,
    baseResolve?: Record<string, unknown>[],
  ) {
    this.manifest = manifest;
    this.resolvers = resolvers ?? { http: new HttpResolver() };
    this.mountOpenerFn = mountOpener ?? this.defaultMountOpener.bind(this);

    // Build base_resolve chain
    const fileBase = getFileBaseResolve(manifest);
    const chain = [...(baseResolve ?? [])];
    if (fileBase) chain.push(fileBase);
    this.baseResolve = chain.length ? chain : undefined;

    this.initMounts();
  }

  static async fromUrl(url: string, opts?: ZMPStoreOptions): Promise<ZMPStore> {
    const manifest = await Manifest.open(url);
    const fileBase = getFileBaseResolve(manifest);
    let baseResolve = opts?.baseResolve;
    if (!baseResolve && !fileBase) {
      const parentUrl = url.slice(0, url.lastIndexOf("/") + 1);
      baseResolve = [{ http: { url: parentUrl } }];
    }
    return new ZMPStore(manifest, opts?.resolvers, opts?.mountOpener, baseResolve);
  }

  static async fromFile(path: string, opts?: ZMPStoreOptions): Promise<ZMPStore> {
    const manifest = await Manifest.open(path);
    const fileBase = getFileBaseResolve(manifest);
    let baseResolve = opts?.baseResolve;
    if (!baseResolve && !fileBase) {
      // Use parent directory as location base
      const idx = path.lastIndexOf("/");
      const parentDir = idx >= 0 ? path.slice(0, idx + 1) : "./";
      baseResolve = [{ http: { url: parentDir } }];
    }
    return new ZMPStore(manifest, opts?.resolvers, opts?.mountOpener, baseResolve);
  }

  static fromManifest(manifest: Manifest, opts?: ZMPStoreOptions): ZMPStore {
    return new ZMPStore(manifest, opts?.resolvers, opts?.mountOpener, opts?.baseResolve);
  }

  getManifest(): Manifest {
    return this.manifest;
  }

  // --- Mounts ---

  private initMounts(): void {
    const mounts: ZPath[] = [];
    const links = new Map<ZPath, ZPath>();

    for (const p of this.manifest.listPaths()) {
      if (p === "") continue;
      const entry = this.manifest.getEntry(p);
      if (!entry || !entry.addressing.includes(Addressing.FOLDER)) continue;

      const zp = new ZPath(p);
      if (entry.addressing.includes(Addressing.MOUNT)) {
        mounts.push(zp);
      } else if (entry.addressing.includes(Addressing.LINK) && entry.resolve) {
        const resolve = JSON.parse(entry.resolve);
        const target = resolve._path?.target;
        if (target) links.set(zp, new ZPath(target));
      }
    }

    this.mountPrefixes = mounts.sort((a, b) => b.depth - a.depth);
    this.linkPrefixes = new Map(
      [...links.entries()].sort((a, b) => b[0].depth - a[0].depth),
    );
  }

  private findMount(key: ZPath): [ZPath, string] | undefined {
    for (const mount of this.mountPrefixes) {
      if (key.isChildOf(mount)) return [mount, key.relativeTo(mount)];
    }
    return undefined;
  }

  private findDirLink(key: ZPath): ZPath | undefined {
    for (const [prefix, target] of this.linkPrefixes) {
      if (key.isChildOf(prefix)) {
        return target.join(key.relativeTo(prefix));
      }
    }
    return undefined;
  }

  private async defaultMountOpener(entry: ManifestEntry): Promise<ZMPStore> {
    if (!entry.resolve) throw new Error(`Mount "${entry.path}" has no resolve`);
    const resolve = JSON.parse(entry.resolve);
    const http = resolve.http;
    if (!http?.url) throw new Error(`Mount "${entry.path}" has no HTTP URL`);

    let url = http.url as string;
    // Resolve relative URL against base
    if (!url.includes("://") && !url.startsWith("/") && this.baseResolve) {
      for (const base of [...this.baseResolve].reverse()) {
        const baseHttp = (base as Record<string, unknown>).http as Record<string, unknown> | undefined;
        if (baseHttp?.url) {
          const baseUrl = baseHttp.url as string;
          if (baseUrl.startsWith("http://") || baseUrl.startsWith("https://")) {
            url = new URL(url, baseUrl).href;
          }
          break;
        }
      }
    }

    return ZMPStore.fromUrl(url, {
      resolvers: this.resolvers,
      mountOpener: this.mountOpenerFn,
    });
  }

  private async getMountStore(mount: ZPath): Promise<ZMPStore> {
    const key = mount.toString();
    const cached = this.mounts.get(key);
    if (cached) return cached;

    const entry = this.manifest.getEntry(mount.toZarr());
    if (!entry) throw new Error(`Mount "${mount}" not found`);
    const store = await this.mountOpenerFn(entry);
    this.mounts.set(key, store);
    return store;
  }

  private isAnnotation(key: ZPath): boolean {
    if (key.isRoot) return true;
    return this.manifest.isAnnotation(key.toString());
  }

  // --- Chunk size for edge padding ---

  private getChunkByteSize(key: ZPath): number | undefined {
    const parts = key.parts;
    const cIdx = parts.indexOf("c");
    if (cIdx < 0) return undefined;
    const arrayPath = parts.slice(0, cIdx).join("/");

    if (this.chunkSizes.has(arrayPath)) {
      return this.chunkSizes.get(arrayPath) ?? undefined;
    }

    for (const metaName of ["zarr.json", ".zarray"]) {
      const metaKey = arrayPath ? `${arrayPath}/${metaName}` : metaName;
      const entry = this.manifest.getEntry(metaKey);
      if (entry?.text) {
        try {
          const meta = JSON.parse(entry.text);
          if (meta.node_type !== "array") continue;
          const chunkShape = meta.chunk_grid?.configuration?.chunk_shape;
          const dtype = meta.data_type;
          if (chunkShape && dtype) {
            const itemSize = dtypeItemSize(dtype);
            if (itemSize) {
              const size = chunkShape.reduce((a: number, b: number) => a * b, 1) * itemSize;
              this.chunkSizes.set(arrayPath, size);
              return size;
            }
          }
        } catch { /* skip */ }
      }
    }

    this.chunkSizes.set(arrayPath, null);
    return undefined;
  }

  // --- AsyncReadable ---

  async get(key: string): Promise<Uint8Array | undefined> {
    const zkey = ZPath.fromZarr(key);

    if (this.isAnnotation(zkey)) return undefined;

    // Mounts
    const mount = this.findMount(zkey);
    if (mount) {
      const [mountPath, subKey] = mount;
      const child = await this.getMountStore(mountPath);
      return child.get(subKey);
    }

    // Links
    const rewritten = this.findDirLink(zkey);
    if (rewritten) return this.get(rewritten.toZarr());

    const entry = this.manifest.getEntry(key);
    if (!entry) return undefined;

    const chain = [...(this.baseResolve ?? [])];
    if (entry.base_resolve) {
      chain.push(JSON.parse(entry.base_resolve));
    }

    let raw = await resolveEntry(
      entry, this.manifest, this.resolvers, chain.length ? chain : undefined,
    );
    if (raw == null) return undefined;

    // Pad edge chunks for content_encoding data
    if (entry.content_encoding) {
      const expected = this.getChunkByteSize(zkey);
      if (expected != null && raw.length < expected) {
        const padded = new Uint8Array(expected);
        padded.set(raw);
        raw = padded;
      }
    }

    return raw;
  }

  async exists(key: string): Promise<boolean> {
    const zkey = ZPath.fromZarr(key);
    if (this.isAnnotation(zkey)) return false;
    const mount = this.findMount(zkey);
    if (mount) {
      const [mountPath, subKey] = mount;
      const child = await this.getMountStore(mountPath);
      return child.exists(subKey);
    }
    const rewritten = this.findDirLink(zkey);
    if (rewritten) return this.exists(rewritten.toZarr());
    return this.manifest.has(key);
  }

  // --- Listing ---

  async *list(): AsyncIterableIterator<string> {
    for (const p of this.manifest.listPaths()) {
      if (p === "") continue;
      const zp = new ZPath(p);
      if (this.isAnnotation(zp)) continue;
      if (this.findMount(zp)) continue;
      if (this.findDirLink(zp)) continue;
      yield zp.toZarr();
    }
    for (const mount of this.mountPrefixes) {
      const child = await this.getMountStore(mount);
      for await (const p of child.list()) {
        yield mount.join(p).toZarr();
      }
    }
  }

  async *listPrefix(prefix: string): AsyncIterableIterator<string> {
    const zprefix = ZPath.fromZarr(prefix);

    const mount = this.findMount(zprefix);
    if (mount) {
      const [mountPath, subPrefix] = mount;
      const child = await this.getMountStore(mountPath);
      for await (const p of child.listPrefix(subPrefix)) {
        yield mountPath.join(p).toZarr();
      }
      return;
    }

    for (const p of this.manifest.listPrefix(zprefix.toString())) {
      if (p === "") continue;
      const zp = new ZPath(p);
      if (this.isAnnotation(zp) || this.findMount(zp)) continue;
      yield zp.toZarr();
    }

    for (const m of this.mountPrefixes) {
      if (m.isEqualOrChildOf(zprefix)) {
        const child = await this.getMountStore(m);
        for await (const p of child.list()) {
          yield m.join(p).toZarr();
        }
      }
    }
  }

  async *listDir(prefix: string): AsyncIterableIterator<string> {
    const zprefix = ZPath.fromZarr(prefix);

    const mount = this.findMount(zprefix);
    if (mount) {
      const [mountPath, subPrefix] = mount;
      const child = await this.getMountStore(mountPath);
      for await (const p of child.listDir(subPrefix)) {
        yield p;
      }
      return;
    }

    const seen = new Set<string>();
    for (const p of this.manifest.listDir(zprefix.toString())) {
      if (p) {
        seen.add(p);
        yield p;
      }
    }

    for (const vdir of [...this.mountPrefixes, ...this.linkPrefixes.keys()]) {
      const child = vdir.childNameUnder(zprefix);
      if (child) {
        const dirEntry = child + "/";
        if (!seen.has(dirEntry)) {
          seen.add(dirEntry);
          yield dirEntry;
        }
      }
    }
  }
}

// --- Helpers ---

function dtypeItemSize(dtype: string): number | undefined {
  const sizes: Record<string, number> = {
    bool: 1,
    int8: 1, uint8: 1,
    int16: 2, uint16: 2,
    int32: 4, uint32: 4, float32: 4,
    int64: 8, uint64: 8, float64: 8,
  };
  return sizes[dtype];
}
