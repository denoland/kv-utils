// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

/**
 * A module to obtain the byte size of a value.
 *
 * @module
 */

import { serialize } from "node:v8";

/**
 * Returns the size, in bytes, of the V8 serialized form of the value, which
 * is used to determine the size of entries being stored in a Deno KV store.
 *
 * This is useful when you want to determine the size of a value before using
 * it as a KV store entry. KV has a key part limit of 2k and a value limit of
 * 64 KB. There are also limits on the total size of atomic operations.
 *
 * > [!NOTE]
 * > There is opaque overhead in using this, as effectively this performs the
 * > same serialization as the KV store does.
 *
 * @param value The value to obtain the size of
 * @returns The size of the value in bytes
 * @example Get the size of a value
 *
 * ```ts
 * import { estimateSize } from "@deno/kv-utils/estimate-size";
 * import { assertEquals } from "@std/assert";
 *
 * const value = { a: new Map([[{ a: 1 }, { b: /234/ }]]), b: false };
 * assertEquals(estimateSize(value), 36);
 * ```
 *
 * @deprecated This function is deprecated and will be removed in a future version.
 *
 * Use V8 serialization directly, for example:
 *
 * ```ts
 * import { serialize } from "node:v8";
 * const value = { a: new Map([[{ a: 1 }, { b: /234/ }]]), b: false };
 * const size = serialize(value).byteLength;
 * ```
 */
export function estimateSize(value: unknown): number {
  try {
    return serialize(value).byteLength;
  } catch {
    // This is to maintain compatibility with the previous implementation
    // which returned 0 for values that could not be serialized.
    return 0;
  }
}
