// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

import { assert } from "@std/assert";

let kv: { close(): void } | undefined;
let path: string | undefined;

/**
 * Creates a temporary `Deno.Kv` instance and returns it.
 *
 * @returns the temporary `Deno.Kv` instance
 */
export async function setup() {
  return kv = await Deno.openKv(path = `${await Deno.makeTempDir()}/test.db`);
}

/**
 * Closes the temporary `Deno.Kv` instance and removes the temporary store.
 *
 * @returns the promise which resolves when the temporary store is removed
 */
export function teardown() {
  assert(kv);
  kv.close();
  assert(path);
  return Deno.remove(path);
}
