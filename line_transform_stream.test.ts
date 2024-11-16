// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

import { assertEquals } from "@std/assert/equals";

import { LinesTransformStream } from "./line_transform_stream.ts";

const fixture =
  `{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}
{"key":[{"type":"string","value":"b"}],"value":{"type":"boolean","value":true},"versionstamp":"000000000000000f0000"}
`;

Deno.test({
  name: "LinesTransformStream",
  async fn() {
    const stream = new Blob([fixture]).stream().pipeThrough(
      new LinesTransformStream(),
    );
    const actual: string[] = [];
    for await (const chunk of stream) {
      actual.push(chunk);
    }
    assertEquals(actual, [
      `{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}`,
      `{"key":[{"type":"string","value":"b"}],"value":{"type":"boolean","value":true},"versionstamp":"000000000000000f0000"}`,
    ]);
  },
});
