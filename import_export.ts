// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

/**
 * Allows for the import and export of data from a {@linkcode Deno.Kv} store.
 *
 * ## Exporting Data
 *
 * Data can be exported from a {@linkcode Deno.Kv} store using the
 * {@linkcode exportEntries} function. This function will return a stream of
 * newline-delimited JSON records that can be consumed by a client. The exported
 * data can be returned as a stream of bytes, a stream of strings or as a
 * {@linkcode Response} with the exported data as the body of the response.
 *
 * ### Example: Exporting Data as a Stream of Bytes
 *
 * ```ts
 * import { exportEntries } from "@deno/kv-utils/import-export";
 * import { assert } from "@std/assert";
 *
 * const db = await Deno.openKv();
 * const stream = exportEntries(db, { prefix: ["person"] });
 * for await (const chunk of stream) {
 *   assert(chunk.byteLength);
 * }
 * db.close();
 * ```
 *
 * ### Example: Exporting Data as a Stream of Strings
 *
 * ```ts
 * import { exportEntries } from "@deno/kv-utils/import-export";
 * import { assert } from "@std/assert";
 *
 * const db = await Deno.openKv();
 * const stream = exportEntries(db, { prefix: ["person"] }, { type: "string" });
 * for await (const chunk of stream) {
 *   assert(typeof chunk === "string");
 * }
 * db.close();
 * ```
 *
 * ### Example: Exporting Data as a Response
 *
 * ```ts ignore
 * import { exportEntries } from "@deno/kv-utils/import-export";
 *
 * const db = await Deno.openKv();
 * const server = Deno.serve((_req) => exportEntries(
 *   db,
 *   { prefix: ["person"] },
 *   { type: "response" }
 * ));
 *
 * await server.finished;
 * db.close();
 * ```
 *
 * ## Importing Data
 *
 * Data can be imported into a {@linkcode Deno.Kv} store using the
 * {@linkcode importEntries} function. This function will read a stream of
 * newline-delimited JSON records and import them into the store. The import
 * process can be controlled with options to overwrite existing entries, provide
 * a prefix for the imported keys, and to handle errors that occur during the
 * import process.
 *
 * ### Example: Importing Data from a Byte Array
 *
 * ```ts
 * import { importEntries } from "@deno/kv-utils/import-export";
 * import { assert } from "@std/assert";
 *
 * const db = await Deno.openKv();
 * const data = new TextEncoder().encode('{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}\n');
 * const result = await importEntries(db, data);
 * assert(result.count === 1);
 * db.close();
 * ```
 *
 * @module
 */

import { entryToJSON, type KvEntryJSON, toKey, toValue } from "./json.ts";
import { LinesTransformStream } from "./line_transform_stream.ts";

/**
 * Options which can be set when calling {@linkcode exportEntries} to export
 * entries as a stream of string records.
 */
export interface ExportEntriesOptionsString extends Deno.KvListOptions {
  /**
   * Determines if the store should be closed after the export is complete.
   *
   * @default false
   */
  close?: boolean | undefined;
  /**
   * The type of export to perform. Where `"bytes"` is the default, this option
   * can be set to `"string"` to export each entry as a stringified JSON object
   * followed by a newline character or `"response"` to return a
   * {@linkcode Response} with the exported data as the body of the
   * {@linkcode Response}.
   *
   * @default "bytes"
   */
  type: "string";
}

/**
 * Options which can be set when calling {@linkcode exportEntries} to export
 * entries as a {@linkcode Response} with the exported data as the body of the
 * {@linkcode Response}.
 */
export interface ExportEntriesOptionsResponse extends Deno.KvListOptions {
  /**
   * Determines if the store should be closed after the export is complete.
   *
   * @default false
   */
  close?: boolean | undefined;
  /**
   * The type of export to perform. Where `"bytes"` is the default, this option
   * can be set to `"string"` to export each entry as a stringified JSON object
   * followed by a newline character or `"response"` to return a
   * {@linkcode Response} with the exported data as the body of the
   * {@linkcode Response}.
   *
   * @default "bytes"
   */
  type: "response";
  /**
   * The filename to use when exporting the data. This is used to set the
   * `Content-Disposition` header in the response which suggests a filename to
   * the client.
   */
  filename?: string | undefined;
}

/**
 * Options which can be set when calling {@linkcode exportEntries} to export
 * entries as a stream of bytes (`Uint8Array` chunks).
 *
 * This is the default format for exporting entries.
 */
export interface ExportEntriesOptionsBytes extends Deno.KvListOptions {
  /**
   * Determines if the store should be closed after the export is complete.
   *
   * @default false
   */
  close?: boolean | undefined;
  /**
   * The type of export to perform. Where `"bytes"` is the default, this option
   * can be set to `"string"` to export each entry as a stringified JSON object
   * followed by a newline character or `"response"` to return a
   * {@linkcode Response} with the exported data as the body of the
   * {@linkcode Response}.
   *
   * @default "bytes"
   */
  type?: "bytes" | undefined;
}

/**
 * Options which can be set when calling {@linkcode exportEntries}.
 */
export type ExportEntriesOptions =
  | ExportEntriesOptionsString
  | ExportEntriesOptionsResponse
  | ExportEntriesOptionsBytes;

/**
 * Options which are supplied when creating an {@linkcode ImportError}.
 *
 * @private
 */
interface ImportErrorOptions extends ErrorOptions {
  count: number;
  db: Deno.Kv;
  errors: number;
  json?: string;
  skipped: number;
}

/**
 * Options which can be set when calling {@linkcode importEntries}.
 */
export interface ImportEntriesOptions {
  /**
   * Determines what happens when a key already exists in the target store for
   * an entry being being import. By default the entry will be skipped. Setting
   * the `overwrite` option to `true` will cause any existing value to be
   * overwritten with the imported value.
   */
  overwrite?: boolean;
  /**
   * An optional callback which occurs when an error is encountered when
   * importing entries. The supplied error will provide details about what was
   * occurring.
   *
   * See {@linkcode ImportError} for more details.
   */
  onError?: (error: ImportError) => void;
  /**
   * An optional callback which occurs every time an entry has been successfully
   * processed, providing an update of the number of entries processed, the
   * number of those that were skipped and the number of those that errored.
   */
  onProgress?: (count: number, skipped: number, errors: number) => void;
  /**
   * The prefix which should be prepended to the front of each entry key when
   * importing. This makes it useful to "namespace" imported data. For example
   * if you were bring in a data set of people, you might supply the
   * {@linkcode Deno.KvKey} of `["person"]`. The imported entry key of `[1]`
   * would then become `["person", 1]`.
   */
  prefix?: Deno.KvKey;
  /**
   * Used to stop the import process. When the signal is aborted, the current
   * import entry will be completed and then the function will return.
   */
  signal?: AbortSignal;
  /**
   * By default, {@linkcode importEntries} will not throw on errors that occur
   * while processing the import data, but just increment the `errors` value
   * and call the `onError()` callback if provided.
   *
   * By setting this to `true`, an {@linkcode ImportError} will be thrown when
   * an error is encountered and terminate the import process.
   */
  throwOnError?: boolean;
}

/**
 * The result returned from calling {@linkcode importEntries}.
 */
export interface ImportEntriesResult {
  /** If set, the import process was aborted prior to completing. */
  aborted?: true;
  /** The number of entries read from the input data. */
  count: number;
  /**
   * The number of entries skipped from the input data. Entries are skipped
   * if a matching entry key is already present in the target, unless the
   * `overwrite` option is set to `true`.
   */
  skipped: number;
  /** The number of entries that errored while processing the data. */
  errors: number;
}

/** The filename extension for NDJSON files. */
const EXT_NDJSON = ".ndjson";

/** The media type for NDJSON which is a newline-delimited JSON format. */
export const MEDIA_TYPE_NDJSON = "application/x-ndjson";
/** The media type for JSONL which is compatible with NDJSON. */
export const MEDIA_TYPE_JSONL = "application/jsonl";
/** The media type for JSON Lines which is compatible with NDJSON. */
export const MEDIA_TYPE_JSON_LINES = "application/json-lines";

const encoder = new TextEncoder();

/**
 * Exports entries from a {@linkcode Deno.Kv} store as a {@linkcode Response}
 * where the body of the response is a stream of newline-delimited JSON records
 * that match the provided selector.
 *
 * @param db The {@linkcode Deno.Kv} store to export entries from.
 * @param selector A selector that selects the range of data returned by a list
 * operation on a {@linkcode Deno.Kv}.
 * @param options Options which can be set
 */
export function exportEntries(
  db: Deno.Kv,
  selector: Deno.KvListSelector,
  options: ExportEntriesOptionsResponse,
): Response;
/**
 * Exports entries from a {@linkcode Deno.Kv} store as a stream of newline-
 * delimited JSON strings that match the provided selector.
 *
 * @param db The {@linkcode Deno.Kv} store to export entries from.
 * @param selector A selector that selects the range of data returned by a list
 * operation on a {@linkcode Deno.Kv}.
 * @param options Options which can be set
 */
export function exportEntries(
  db: Deno.Kv,
  selector: Deno.KvListSelector,
  options: ExportEntriesOptionsString,
): ReadableStream<string>;
/**
 * Exports entries from a {@linkcode Deno.Kv} store as a stream of newline-
 * delimited JSON records encoded as bytes (`Uint8Array` chunks) that match the
 * provided selector.
 *
 * @param db The {@linkcode Deno.Kv} store to export entries from.
 * @param selector A selector that selects the range of data returned by a list
 * operation on a {@linkcode Deno.Kv}.
 * @param options Options which can be set
 */
export function exportEntries(
  db: Deno.Kv,
  selector: Deno.KvListSelector,
  options?: ExportEntriesOptions,
): ReadableStream<Uint8Array>;
export function exportEntries(
  db: Deno.Kv,
  selector: Deno.KvListSelector,
  options: ExportEntriesOptions = {},
): ReadableStream<Uint8Array | string> | Response {
  const text = options.type === "string";
  let cancelled = false;
  const stream = new ReadableStream({
    async start(controller) {
      try {
        for await (const entry of db.list(selector, options)) {
          const chunk = entryToJSON(entry);
          controller.enqueue(
            text
              ? `${JSON.stringify(chunk)}\n`
              : encoder.encode(`${JSON.stringify(chunk)}\n`),
          );
          if (cancelled) {
            return;
          }
        }
        if (options.close) {
          db.close();
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
    cancel(_reason) {
      cancelled = true;
    },
  });
  if (options.type === "response") {
    const init = {
      headers: { "content-type": MEDIA_TYPE_NDJSON } as Record<string, string>,
    };
    if (options.filename) {
      init.headers["content-disposition"] =
        `attachment; filename="${options.filename}${EXT_NDJSON}"`;
    }
    return new Response(stream, init);
  }
  return stream;
}

/**
 * An error that can occur when importing records into a {@linkcode Deno.Kv}
 * store. Information associated with the error is available with the `cause`
 * being set to the original error that was thrown.
 */
export class ImportError extends Error {
  #count: number;
  #db: Deno.Kv;
  #errors: number;
  #json?: string;
  #skipped: number;

  /**
   * The number of entries that had been read from the stream when the
   * error occurred.
   */
  get count(): number {
    return this.#count;
  }
  /**
   * Reference to the {@linkcode Deno.Kv} store that was the target for the
   * import.
   */
  get db(): Deno.Kv {
    return this.#db;
  }
  /**
   * The number of errors in aggregate that had occurred to this point.
   */
  get errors(): number {
    return this.#errors;
  }
  /**
   * If available, the most recent JSON string what had been read from the data.
   */
  get json(): string | undefined {
    return this.#json;
  }
  /**
   * The aggregate number of records that had been skipped.
   */
  get skipped(): number {
    return this.#skipped;
  }

  constructor(
    message: string,
    { count, errors, json, db, skipped, ...options }: ImportErrorOptions,
  ) {
    super(message, options);
    this.#count = count;
    this.#errors = errors;
    this.#json = json;
    this.#db = db;
    this.#skipped = skipped;
  }
}

/**
 * Imports entries into a {@linkcode Deno.Kv} store from a stream of newline-
 * delimited JSON records. The import process can be controlled with options to
 * overwrite existing entries, provide a prefix for the imported keys, and to
 * handle errors that occur during the import process.
 *
 * The import process will read the stream of records and parse each record as
 * JSON. The key and value of the entry will be extracted from the JSON object
 * and imported into the store. If the `overwrite` option is set to `true`, then
 * any existing entry with the same key will be replaced with the imported
 * value.
 *
 * If an error occurs while processing the import data, the error will be passed
 * to the `onError` callback if provided. If the `throwOnError` option is set to
 * `true`, then an {@linkcode ImportError} will be thrown when an error is
 * encountered and terminate the import process.
 *
 * @param db The {@linkcode Deno.Kv} store to import entries into.
 * @param data The data to import into the store. This can be a stream of bytes,
 * a {@linkcode Blob}, an {@linkcode ArrayBufferView}, an
 * {@linkcode ArrayBuffer} or a string.
 * @param options Options which can be set when importing entries.
 * @returns A promise that resolves to an {@linkcode ImportEntriesResult} object
 * which provides details about the import process.
 */
export async function importEntries(
  db: Deno.Kv,
  data:
    | ReadableStream<Uint8Array>
    | Blob
    | ArrayBufferView
    | ArrayBuffer
    | string,
  options: ImportEntriesOptions = {},
): Promise<ImportEntriesResult> {
  const {
    overwrite = false,
    prefix = [],
    onError,
    onProgress,
    signal,
    throwOnError,
  } = options;
  let stream: ReadableStream<string>;
  const transformer = new LinesTransformStream();
  if (data instanceof ReadableStream) {
    stream = data.pipeThrough(transformer);
  } else if (data instanceof Blob) {
    stream = data.stream().pipeThrough(transformer);
  } else {
    stream = new Blob([data]).stream().pipeThrough(transformer);
  }
  const reader = stream.getReader();
  let count = 0;
  let errors = 0;
  let skipped = 0;
  while (true) {
    let result: ReadableStreamReadResult<string> | undefined = undefined;
    try {
      result = await reader.read();
      if (result.value) {
        count++;
        const entry: KvEntryJSON = JSON.parse(result.value);
        const { key, value } = entry;
        const entryKey = prefix.length
          ? [...prefix, ...toKey(key)]
          : toKey(key);
        if (!overwrite) {
          const { versionstamp } = await db.get(entryKey);
          if (versionstamp) {
            skipped++;
            continue;
          }
        }
        await db.set(entryKey, toValue(value));
        onProgress?.(count, skipped, errors);
      }
      if (result.done) {
        break;
      }
      if (signal?.aborted) {
        reader.releaseLock();
        return { aborted: true, count, skipped, errors };
      }
    } catch (cause) {
      errors++;
      if (onError || throwOnError) {
        const error = new ImportError(
          cause instanceof Error ? cause.message : "An import error occurred.",
          { cause, json: result?.value, count, db, skipped, errors },
        );
        onError?.(error);
        if (throwOnError) {
          reader.releaseLock();
          throw error;
        }
      }
    }
  }
  reader.releaseLock();
  return { count, skipped, errors };
}
