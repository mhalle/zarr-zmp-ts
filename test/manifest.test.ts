import { describe, it, expect } from "vitest";
import { Manifest } from "../src/manifest.js";
import { ZPath } from "../src/path.js";

// antscan2.zmp has group at / with array at /volume/
const ZMP_PATH = "/tmp/antscan2.zmp";

describe("Manifest", () => {
  it("opens a zmp file", async () => {
    const m = await Manifest.open(ZMP_PATH);
    expect(m.length).toBeGreaterThan(0);
  });

  it("reads metadata", async () => {
    const m = await Manifest.open(ZMP_PATH);
    expect(m.metadata.zarr_format).toBe("3");
  });

  it("reads archive metadata", async () => {
    const m = await Manifest.open(ZMP_PATH);
    const am = m.archiveMetadata;
    expect(am).toBeDefined();
    expect(am!.format).toBe("TIFF");
    expect(am!.dtype).toBe("uint8");
  });

  it("has entries with absolute paths", async () => {
    const m = await Manifest.open(ZMP_PATH);
    expect(m.has("/zarr.json")).toBe(true);
    expect(m.has("/volume/zarr.json")).toBe(true);
    expect(m.has("/volume/c/0/0/0")).toBe(true);
    // Also accepts bare paths
    expect(m.has("zarr.json")).toBe(true);
    expect(m.has("volume/c/0/0/0")).toBe(true);
  });

  it("gets entry with resolve dict", async () => {
    const m = await Manifest.open(ZMP_PATH);
    const entry = m.getEntry("/volume/c/0/0/0");
    expect(entry).toBeDefined();
    expect(entry!.addressing).toContain("R");
    expect(entry!.resolve).toBeDefined();
    expect(entry!.content_encoding).toBe("zlib");

    const resolve = JSON.parse(entry!.resolve!);
    expect(resolve.http.offset).toBeGreaterThanOrEqual(0);
    expect(resolve.http.length).toBeGreaterThan(0);
  });

  it("lists paths", async () => {
    const m = await Manifest.open(ZMP_PATH);
    const paths = [...m.listPaths()];
    expect(paths.length).toBeGreaterThan(5000);
    expect(paths).toContain("/zarr.json");
  });

  it("lists directory at root", async () => {
    const m = await Manifest.open(ZMP_PATH);
    const top = [...m.listDir("/")];
    expect(top).toContain("volume/");
    expect(top).toContain("zarr.json");
  });

  it("lists directory under volume", async () => {
    const m = await Manifest.open(ZMP_PATH);
    const vol = [...m.listDir("/volume")];
    expect(vol).toContain("zarr.json");
    expect(vol).toContain("c/");
  });

  it("reads inline text", async () => {
    const m = await Manifest.open(ZMP_PATH);
    const entry = m.getEntry("/zarr.json");
    expect(entry).toBeDefined();
    expect(entry!.text).toBeDefined();
    const meta = JSON.parse(entry!.text!);
    expect(meta.zarr_format).toBe(3);
    expect(meta.node_type).toBe("group");
  });

  it("reads array metadata", async () => {
    const m = await Manifest.open(ZMP_PATH);
    const entry = m.getEntry("/volume/zarr.json");
    expect(entry).toBeDefined();
    const meta = JSON.parse(entry!.text!);
    expect(meta.node_type).toBe("array");
    expect(meta.shape).toEqual([1254, 1121, 838]);
  });
});

describe("ZPath", () => {
  it("normalizes paths", () => {
    expect(new ZPath("/a/b").toString()).toBe("/a/b");
    expect(new ZPath("a/b").toString()).toBe("/a/b");
    expect(new ZPath("/a/b/").toString()).toBe("/a/b");
    expect(new ZPath("//a//b").toString()).toBe("/a/b");
  });

  it("converts to/from zarr", () => {
    expect(ZPath.fromZarr("a/b").toString()).toBe("/a/b");
    expect(new ZPath("/a/b").toZarr()).toBe("a/b");
    expect(ZPath.ROOT.toZarr()).toBe("");
    expect(ZPath.fromZarr("").isRoot).toBe(true);
  });

  it("parent and name", () => {
    const p = new ZPath("/a/b/c");
    expect(p.name).toBe("c");
    expect(p.parent.toString()).toBe("/a/b");
    expect(ZPath.ROOT.parent.isRoot).toBe(true);
  });

  it("isChildOf", () => {
    expect(new ZPath("/a/b/c").isChildOf("/a")).toBe(true);
    expect(new ZPath("/a/b/c").isChildOf("/a/b/c")).toBe(false);
    expect(new ZPath("/abc").isChildOf("/a")).toBe(false);
  });

  it("relativeTo", () => {
    expect(new ZPath("/a/b/c").relativeTo("/a")).toBe("b/c");
    expect(new ZPath("/a").relativeTo("/")).toBe("a");
  });

  it("childNameUnder", () => {
    expect(new ZPath("/a/b/c").childNameUnder("/a")).toBe("b");
    expect(new ZPath("/a").childNameUnder("/a")).toBeUndefined();
  });
});
