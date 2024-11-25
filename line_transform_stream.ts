// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

/**
 * Module that provides {@linkcode LinesTransformStream} which is used within
 * the library to transform a byte stream into chunks of string lines.
 *
 * @module
 */

import { concat } from "@std/bytes/concat";

const LF = 0x0a;
const CR = 0x0d;
const decoder = new TextDecoder();

function stripEol(u8: Uint8Array): Uint8Array {
  const length = u8.byteLength;
  if (u8[length - 1] === LF) {
    let drop = 1;
    if (length > 1 && u8[length - 2] === CR) {
      drop = 2;
    }
    return u8.subarray(0, length - drop);
  }
  return u8;
}

/**
 * A transform stream that takes a byte stream and transforms it into a stream
 * of string lines.
 */
export class LinesTransformStream extends TransformStream<Uint8Array, string> {
  #buffer = new Uint8Array(0);
  #pos = 0;

  constructor() {
    super({
      transform: (chunk, controller) => {
        this.#transform(chunk, controller);
      },
      flush: (controller) => {
        const slice = stripEol(this.#buffer.subarray(this.#pos));
        if (slice.length) {
          try {
            controller.enqueue(decoder.decode(slice));
          } catch (error) {
            controller.error(error);
          }
        }
      },
    });
  }

  #readLineBytes(): Uint8Array | null {
    let slice: Uint8Array | null = null;
    const i = this.#buffer.subarray(this.#pos).indexOf(LF);
    if (i >= 0) {
      slice = this.#buffer.subarray(this.#pos, this.#pos + i + 1);
      this.#pos += i + 1;
      return stripEol(slice);
    }
    return null;
  }

  *#lines(): IterableIterator<string | null> {
    while (true) {
      const bytes = this.#readLineBytes();
      if (!bytes) {
        this.#truncate();
        return null;
      }
      yield decoder.decode(bytes);
    }
  }

  #transform(
    chunk: Uint8Array,
    controller: TransformStreamDefaultController<string>,
  ) {
    this.#buffer = concat([this.#buffer, chunk]);
    const iterator = this.#lines();
    while (true) {
      try {
        const result = iterator.next();
        if (result.value) {
          controller.enqueue(result.value);
        }
        if (result.done) {
          break;
        }
      } catch (error) {
        controller.error(error);
      }
    }
  }

  #truncate() {
    this.#buffer = this.#buffer.slice(this.#pos);
    this.#pos = 0;
  }
}
