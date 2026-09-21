import assert from "node:assert/strict";
import test from "node:test";

import { changeSqlConsoleConnection } from "./connectionSelection.js";

test("changing the physical connection clears the previously asserted execution role", () => {
  const next = changeSqlConsoleConnection(
    {
      connId: "source-connection",
      targetRole: "source",
      result: { columns: ["id"], rows: [[1]], rowCount: 1, elapsedMs: 2 },
    },
    "target-connection",
  );

  assert.deepEqual(next, {
    connId: "target-connection",
    targetRole: undefined,
    result: null,
  });
});
