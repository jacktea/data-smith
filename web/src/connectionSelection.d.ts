import type { ExecSqlTargetRole, SqlQueryResult } from "./api";

export interface SqlConsoleConnectionState {
  connId?: string;
  targetRole?: ExecSqlTargetRole;
  result: SqlQueryResult | null;
}

export function changeSqlConsoleConnection(
  current: SqlConsoleConnectionState,
  nextConnectionId: string,
): SqlConsoleConnectionState;
