# zarr-zmp

Read-only Zarr v3 store backed by ZMP manifest parquet files (TypeScript).

ZMP manifests are lightweight parquet files that index data stored elsewhere — byte ranges in TIFF files, zip archives, HTTP endpoints, or inline. This library reads them and exposes the data as a zarr v3 store for use with [zarrita](https://github.com/manzt/zarrita.js).

## Install

```bash
npm install zarr-zmp
```

## Quick start

```typescript
import { ZMPStore } from "zarr-zmp";
import { open, get } from "zarrita";

// Open a .zmp manifest (local path or URL)
const store = await ZMPStore.fromUrl("https://example.com/scan.zmp");

// Use with zarrita
const arr = await open(store, { kind: "array" });
const slice = await get(arr, [0, null, null]);
// slice.data is a typed array of pixel values
```

## Features

- **Virtual references**: chunks are byte-range references into external files (TIFF, zip, HTTP), not copies
- **Content decoding**: transparent decompression of deflate/gzip/zlib data (e.g. TIFF strips)
- **Absolute paths**: manifest paths use leading `/` (`/volume/c/0/0/0`)
- **Zarr boundary conversion**: store accepts bare zarr keys (`volume/c/0/0/0`) and converts internally
- **Mounts and links**: compose manifests by mounting child `.zmp` files at path prefixes
- **Edge chunk padding**: handles partial chunks at array boundaries
- **Archive metadata**: dataset-level metadata accessible via `manifest.archiveMetadata`
- **Parquet via hyparquet**: pure JS parquet reader, works in browser and Node.js

## API

### ZMPStore

```typescript
// From URL (browser or Node.js)
const store = await ZMPStore.fromUrl("https://example.com/data.zmp");

// From local file (Node.js)
const store = await ZMPStore.fromFile("/path/to/data.zmp");

// From an already-loaded manifest
const store = ZMPStore.fromManifest(manifest);

// AsyncReadable interface (zarrita compatible)
const bytes = await store.get("volume/c/0/0/0");
const exists = await store.exists("zarr.json");

// Listing
for await (const path of store.list()) { ... }
for await (const path of store.listPrefix("volume/c/")) { ... }
for await (const child of store.listDir("")) { ... }
```

### Manifest

```typescript
import { Manifest } from "zarr-zmp";

const m = await Manifest.open("data.zmp");

// Lookup
m.has("/zarr.json");              // true
m.getEntry("/volume/c/0/0/0");   // ManifestEntry

// Archive metadata
m.archiveMetadata;                // { format: "TIFF", shape: [...], ... }

// Listing
[...m.listPaths()];              // all paths
[...m.listDir("/")];             // top-level: ["zarr.json", "volume/"]
[...m.listPrefix("/volume/c")];  // all chunks
```

### ZPath

```typescript
import { ZPath } from "zarr-zmp";

const p = new ZPath("/volume/c/0/0");
p.name;                          // "0"
p.parent.toString();             // "/volume/c/0"
p.parts;                         // ["volume", "c", "0", "0"]
p.isChildOf("/volume");          // true
p.relativeTo("/volume");         // "c/0/0"

// Zarr conversion
ZPath.fromZarr("volume/c/0/0"); // ZPath("/volume/c/0/0")
p.toZarr();                      // "volume/c/0/0"
```

## Creating .zmp files

Use the Python [zmanifest](https://github.com/mhalle/zmanifest) CLI:

```bash
# From a TIFF (virtual — 70KB manifest for a 623MB file)
zmp import-tiff scan.tif output.zmp

# From a zip archive
zmp import-zip data.zarr.zip output.zmp

# Inspect
zmp info output.zmp
zmp list -l output.zmp
zmp show output.zmp /volume/c/0/0/0
```

## Dependencies

- [hyparquet](https://github.com/hyparam/hyparquet) — pure JS parquet reader
- [hyparquet-compressors](https://github.com/hyparam/hyparquet-compressors) — zstd/snappy/etc for parquet
- [fflate](https://github.com/101arrowz/fflate) — deflate/gzip/zlib decompression for content_encoding

## License

BSD-3-Clause
