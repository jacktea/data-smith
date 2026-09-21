/**
 * Changing the physical database invalidates the source/target assertion that
 * was made for the previous connection. Keep this transition in one place so
 * every connection change clears both the query result and the execution role.
 */
export function changeSqlConsoleConnection(_current, nextConnectionId) {
  return {
    connId: nextConnectionId,
    targetRole: undefined,
    result: null,
  };
}
