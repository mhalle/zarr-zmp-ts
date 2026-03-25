/**
 * Immutable, absolute, `/`-separated archive path.
 *
 * Root is "/". All paths start with "/". No trailing slashes except root.
 */
export class ZPath {
  static readonly ROOT = new ZPath("/");

  private readonly _path: string;

  constructor(path = "/") {
    this._path = normalize(path);
  }

  /** Create from a zarr-style bare path (no leading `/`). */
  static fromZarr(zarrPath: string): ZPath {
    if (zarrPath === "" || zarrPath === "/") return ZPath.ROOT;
    return new ZPath("/" + zarrPath.replace(/^\/+/, ""));
  }

  /** Return the zarr-style bare path (no leading `/`). */
  toZarr(): string {
    if (this._path === "/") return "";
    return this._path.slice(1);
  }

  toString(): string {
    return this._path;
  }

  get name(): string {
    if (this._path === "/") return "";
    const idx = this._path.lastIndexOf("/");
    return this._path.slice(idx + 1);
  }

  get parent(): ZPath {
    if (this._path === "/") return this;
    const idx = this._path.lastIndexOf("/");
    return new ZPath(idx === 0 ? "/" : this._path.slice(0, idx));
  }

  get parts(): string[] {
    if (this._path === "/") return [];
    return this._path.slice(1).split("/");
  }

  get isRoot(): boolean {
    return this._path === "/";
  }

  get depth(): number {
    if (this._path === "/") return 0;
    return this._path.split("/").length - 1;
  }

  /** Join: `new ZPath("/a").join("b")` → `ZPath("/a/b")`. */
  join(other: string | ZPath): ZPath {
    const child = String(other).replace(/^\/+|\/+$/g, "");
    if (!child) return this;
    if (this._path === "/") return new ZPath("/" + child);
    return new ZPath(this._path + "/" + child);
  }

  equals(other: ZPath | string): boolean {
    const o = typeof other === "string" ? normalize(other) : other._path;
    return this._path === o;
  }

  isChildOf(ancestor: ZPath | string): boolean {
    const a = typeof ancestor === "string" ? normalize(ancestor) : ancestor._path;
    if (a === "/") return this._path !== "/";
    return this._path.startsWith(a + "/");
  }

  isEqualOrChildOf(ancestor: ZPath | string): boolean {
    return this.equals(ancestor) || this.isChildOf(ancestor);
  }

  relativeTo(ancestor: ZPath | string): string {
    const a = typeof ancestor === "string" ? normalize(ancestor) : ancestor._path;
    if (a === "/") {
      if (this._path === "/") return "";
      return this._path.slice(1);
    }
    const prefix = a + "/";
    if (!this._path.startsWith(prefix)) {
      throw new Error(`${this._path} is not under ${a}`);
    }
    return this._path.slice(prefix.length);
  }

  childNameUnder(ancestor: ZPath | string): string | undefined {
    try {
      const rel = this.relativeTo(ancestor);
      if (!rel) return undefined;
      return rel.split("/")[0];
    } catch {
      return undefined;
    }
  }
}

function normalize(path: string): string {
  if (!path || path === "/") return "/";
  if (!path.startsWith("/")) path = "/" + path;
  path = path.replace(/\/+$/, "");
  while (path.includes("//")) path = path.replace(/\/\//g, "/");
  return path || "/";
}
