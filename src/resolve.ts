/**
 * Content resolution for manifest entries.
 */

import type { Manifest } from "./manifest.js";
import type { ManifestEntry, Resolver, ResolveDict } from "./types.js";
import { Addressing } from "./types.js";
import { decodeContent } from "./decode.js";

/**
 * Collect per-scheme base dicts from the chain.
 */
function collectSchemeBases(
  scheme: string,
  baseChain: Record<string, unknown>[] | undefined,
): Record<string, unknown>[] {
  if (!baseChain) return [];
  const bases: Record<string, unknown>[] = [];
  for (const layer of baseChain) {
    const val = layer[scheme] as Record<string, unknown> | undefined;
    if (val) bases.push(val);
  }
  return bases;
}

/**
 * Get the file-level base_resolve from manifest metadata.
 */
export function getFileBaseResolve(
  manifest: Manifest,
): Record<string, unknown> | undefined {
  const extra = manifest.metadata.extra;
  if (!extra) return undefined;
  const raw = extra.base_resolve;
  if (raw == null) return undefined;
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch {
      return undefined;
    }
  }
  return raw as Record<string, unknown>;
}

/**
 * Resolve content for a manifest entry.
 *
 * Resolution order:
 * 1. Inline text (T)
 * 2. Inline data (D/Z)
 * 3. Link — follow target (L)
 * 4. Resolve — iterate schemes (R)
 *
 * Content encoding is applied transparently.
 */
export async function resolveEntry(
  entry: ManifestEntry,
  manifest: Manifest,
  resolvers?: Record<string, Resolver>,
  baseResolve?: Record<string, unknown>[],
  visited?: Set<string>,
): Promise<Uint8Array | undefined> {
  const flags = entry.addressing;
  const encoding = entry.content_encoding;

  // 1. Inline text
  if (flags.includes(Addressing.TEXT) && entry.text != null) {
    return new TextEncoder().encode(entry.text);
  }

  // 2. Inline data
  if (flags.includes(Addressing.DATA) || flags.includes(Addressing.DATA_Z)) {
    const data = await manifest.getData(entry.path);
    if (data != null) return decodeContent(data, encoding);
  }

  // 3. Link — follow target
  if (flags.includes(Addressing.LINK) && entry.resolve != null) {
    const resolveDict = JSON.parse(entry.resolve) as ResolveDict;
    const pathParams = resolveDict._path as Record<string, unknown> | undefined;
    if (pathParams?.target) {
      const v = visited ?? new Set<string>();
      if (v.has(entry.path)) {
        throw new Error(`Circular link: ${entry.path} → ${pathParams.target}`);
      }
      v.add(entry.path);
      const targetEntry = manifest.getEntry(pathParams.target as string);
      if (targetEntry) {
        const chain = [...(baseResolve ?? [])];
        if (targetEntry.base_resolve) {
          chain.push(JSON.parse(targetEntry.base_resolve));
        }
        return resolveEntry(targetEntry, manifest, resolvers, chain, v);
      }
    }
  }

  // 4. Resolve — try each scheme
  if (flags.includes(Addressing.RESOLVE) && entry.resolve != null && resolvers) {
    const resolveDict = JSON.parse(entry.resolve) as ResolveDict;
    for (const [scheme, params] of Object.entries(resolveDict)) {
      if (scheme.startsWith("_")) continue;
      const resolver = resolvers[scheme];
      if (!resolver) continue;
      const schemeBases = collectSchemeBases(scheme, baseResolve);
      const result = await resolver.resolve(params, schemeBases.length ? schemeBases : undefined);
      if (result != null) return decodeContent(result, encoding);
    }
  }

  return undefined;
}
