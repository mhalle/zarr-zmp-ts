import type { Resolver } from "./types.js";

/**
 * Fetch bytes from a local path or HTTP(S) URL with optional byte range.
 */
async function fetchBytes(
  url: string,
  offset?: number,
  length?: number,
): Promise<Uint8Array | undefined> {
  if (url.startsWith("http://") || url.startsWith("https://")) {
    const headers: Record<string, string> = {};
    if (offset != null && length != null) {
      headers["Range"] = `bytes=${offset}-${offset + length - 1}`;
    }
    const resp = await fetch(url, { headers });
    if (resp.status === 200 || resp.status === 206) {
      return new Uint8Array(await resp.arrayBuffer());
    }
    return undefined;
  }

  // Local file path
  let path = url;
  if (path.startsWith("file://")) path = path.slice(7);

  try {
    const fs = await import("node:fs/promises");
    const fh = await fs.open(path, "r");
    try {
      if (offset != null && length != null) {
        const buf = Buffer.alloc(length);
        await fh.read(buf, 0, length, offset);
        return new Uint8Array(buf);
      }
      const buf = await fh.readFile();
      return new Uint8Array(buf);
    } finally {
      await fh.close();
    }
  } catch {
    return undefined;
  }
}

/**
 * Resolves content via HTTP(S) URLs or local file paths.
 *
 * Handles relative URLs via base params, byte ranges via offset/length.
 * Matches the Python HttpResolver behavior.
 */
export class HttpResolver implements Resolver {
  async resolve(
    params: Record<string, unknown>,
    bases?: Record<string, unknown>[],
  ): Promise<Uint8Array | undefined> {
    let url = (params.url as string) ?? "";
    if (!url && !bases?.length) return undefined;

    // Compose the base chain
    if (bases?.length) {
      let effectiveBase: string | undefined;
      for (const base of bases) {
        const baseUrl = base.url as string | undefined;
        if (baseUrl == null) continue;
        if (
          effectiveBase == null ||
          baseUrl.includes("://") ||
          baseUrl.startsWith("/")
        ) {
          effectiveBase = baseUrl;
        } else if (
          effectiveBase.startsWith("http://") ||
          effectiveBase.startsWith("https://")
        ) {
          effectiveBase = new URL(baseUrl, effectiveBase).href;
        } else {
          // Filesystem join
          const parts = (effectiveBase + "/" + baseUrl).split("/");
          const resolved: string[] = [];
          for (const p of parts) {
            if (p === "..") resolved.pop();
            else if (p !== "." && p !== "") resolved.push(p);
          }
          effectiveBase = "/" + resolved.join("/");
        }
      }

      // Resolve entry URL against base
      if (effectiveBase && url) {
        if (!url.includes("://") && !url.startsWith("/")) {
          if (
            effectiveBase.startsWith("http://") ||
            effectiveBase.startsWith("https://")
          ) {
            url = new URL(url, effectiveBase).href;
          } else {
            const parts = (effectiveBase + "/" + url).split("/");
            const resolved: string[] = [];
            for (const p of parts) {
              if (p === "..") resolved.pop();
              else if (p !== "." && p !== "") resolved.push(p);
            }
            url = "/" + resolved.join("/");
          }
        }
      } else if (effectiveBase && !url) {
        url = effectiveBase;
      }
    }

    const offset = params.offset as number | undefined;
    const length = params.length as number | undefined;
    return fetchBytes(url, offset, length);
  }
}
