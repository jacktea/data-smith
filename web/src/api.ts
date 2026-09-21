// 统一 fetch 封装与接口类型 —— 唯一事实来源:web-contract.md 第 2 节(勿单方更改)

const BASE = "/api";
const enc = encodeURIComponent;

export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function qs(params: Record<string, string | undefined>): string {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== "") sp.set(k, v);
  }
  const s = sp.toString();
  return s ? `?${s}` : "";
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(BASE + path, {
      ...init,
      headers: init?.body != null ? { "Content-Type": "application/json" } : undefined,
    });
  } catch {
    throw new ApiError(0, "网络错误:无法连接服务端,请确认后端已启动(:8080)");
  }
  if (!res.ok) {
    let msg = `请求失败(HTTP ${res.status})`;
    try {
      const body = (await res.json()) as { error?: unknown };
      if (typeof body?.error === "string" && body.error) msg = body.error;
    } catch {
      // 非 JSON 错误体,保留默认消息
    }
    throw new ApiError(res.status, msg);
  }
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

// ---------------- 连接 ----------------

export type DbType = "mysql" | "postgres";
export type TableKind = "table" | "view" | "all";

export interface ProxyView {
  host: string;
  port: number;
  user: string;
  type: string;
  knownHostsPath: string;
  hostFingerprint: string;
  rsaKeyPathSet: boolean;
  passSet: boolean;
}

export interface Connection {
  id: string;
  name: string;
  type: DbType;
  host: string;
  port: number;
  user: string;
  passwordSet: boolean;
  dbname: string;
  tableSchema: string;
  ssl: boolean;
  proxy: ProxyView | null;
  connectTimeoutMs: number;
  maxOpenConns: number;
}

export interface ProxyInput {
  type: string;
  host: string;
  port: number;
  user: string;
  /** PUT/POST 缺省 = 保持原值 */
  password?: string;
  knownHostsPath?: string;
  hostFingerprint?: string;
  /** PUT 缺省 = 保持原值 */
  rsaKeyPath?: string;
}

export interface ConnectionInput {
  name: string;
  type: DbType;
  host: string;
  port: number;
  user: string;
  /** PUT 缺省 = 保持原密码 */
  password?: string;
  dbname: string;
  tableSchema: string;
  ssl: boolean;
  proxy?: ProxyInput | null;
  connectTimeoutMs: number;
  maxOpenConns: number;
}

export interface TestResult {
  ok: boolean;
  error: string;
  serverVersion: string;
}

export interface ColumnInfo {
  name: string;
  dataType: string;
  nullable: boolean;
  comment: string;
  primaryKey: boolean;
  position: number;
}

export interface TableInfo {
  name: string;
  type: string;
  columns: ColumnInfo[];
}

// ---------------- 比对方案 ----------------

export interface SchemeTable {
  table: string;
  columns: string[];
  ignoreColumns: string[];
}

export interface Scheme {
  id: string;
  name: string;
  updatedAt: string;
  tables: SchemeTable[];
}

// ---------------- 任务 ----------------

export type JobType = "diff-schema" | "diff-data" | "diff-full" | "exec-sql" | "reset" | "migrate" | "rollback";
export type JobStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

export interface JobProgress {
  currentTable: string;
  tablesDone: number;
  tablesTotal: number;
}

export interface ColumnMod {
  name: string;
  detail: string;
}

export interface TableModSummary {
  table: string;
  columnsAdded: string[];
  columnsDropped: string[];
  /** server 实际形状:{name,detail}[](契约文本为 string[],以 server 实现为准) */
  columnsModified: ColumnMod[];
  primaryKeyChanged: boolean;
  commentChanged: boolean;
}

export interface SchemaDiffSummary {
  tablesAdded: string[];
  tablesDropped: string[];
  tablesModified: TableModSummary[];
  destructive: boolean;
}

export interface TableDiffSummary {
  table: string;
  added: number;
  modified: number;
  dropped: number;
  status: "ok" | "failed";
  error: string;
}

export interface SummaryDiffSchema {
  schema: SchemaDiffSummary;
  files: string[];
}
export interface SummaryDiffData {
  complete: boolean;
  tables: TableDiffSummary[];
  files: string[];
}
export interface SummaryDiffFull {
  schema: SchemaDiffSummary;
  complete: boolean;
  tables: TableDiffSummary[];
  skippedTables: string[];
  files: string[];
  /** 一步登记成功时才有 */
  registeredVersion?: string;
  upFile?: string;
  downFile?: string;
}
export interface SummaryExecSql {
  mode: string;
  ok: boolean;
}
export interface SummaryReset {
  ok: boolean;
}
export interface SummaryMigrate {
  dryRun: boolean;
  applied: { version: string; title: string }[];
  targetVersion: string;
}
export interface SummaryRollback {
  /** 旧字段:单步回退(回退最新)时保留 */
  version?: string;
  /** 依次回退的版本,最新在前 */
  versions?: string[];
  targetVersion?: string;
  rolled: boolean;
}

export type JobSummary =
  | SummaryDiffSchema
  | SummaryDiffData
  | SummaryDiffFull
  | SummaryExecSql
  | SummaryReset
  | SummaryMigrate
  | SummaryRollback;

export interface Job {
  id: string;
  type: JobType;
  status: JobStatus;
  createdAt: string;
  startedAt: string;
  finishedAt: string;
  /** 服务端已掩码 */
  params: Record<string, unknown>;
  summary: JobSummary | null;
  error: string;
  log: string[];
  progress: JobProgress | null;
}

export interface Artifact {
  name: string;
  size: number;
}

export interface DiffSchemaJobInput {
  sourceId: string;
  targetId: string;
  includeTables: string[];
  excludeTables: string[];
}

export interface DiffDataTableRule {
  table: string;
  columns: string[];
  ignoreColumns: string[];
}

export interface DiffDataJobInput {
  sourceId: string;
  targetId: string;
  schemeId?: string;
  tables: DiffDataTableRule[];
  batchSize?: number;
  chunkSize?: number;
  dmlBatchSize?: number;
  chunkHash?: boolean;
  bestEffort?: boolean;
}

export interface DiffFullRegisterInput {
  libraryId: string;
  /** 留空 = 自动取脚本库下一个版本 */
  version?: string;
  title: string;
  expectedConnectionId?: string;
}

/** diff-full 数据比对模式:auto=引擎自动判定影子事务;shadow=强制影子(仅 PG source);direct=直接比对 */
export type DataDiffMode = "auto" | "shadow" | "direct";

export interface DiffFullJobInput {
  sourceId: string;
  targetId: string;
  includeTables?: string[];
  excludeTables?: string[];
  schemeId?: string;
  tables: DiffDataTableRule[];
  batchSize?: number;
  chunkSize?: number;
  dmlBatchSize?: number;
  chunkHash?: boolean;
  bestEffort?: boolean;
  /** 数据比对模式,缺省 auto(引擎默认:PG source 且有结构差异时影子两阶段) */
  dataDiffMode?: DataDiffMode;
  /** 可选:比对成功后一步登记为脚本库迁移版本 */
  register?: DiffFullRegisterInput;
}

export type ExecSqlMode = "dryrun" | "tx" | "direct";
export type ExecSqlTargetRole = "source" | "target";

export interface ExecSqlJobInput {
  connectionId: string;
  content: string;
  mode: ExecSqlMode;
  targetRole: ExecSqlTargetRole;
}

export interface MigrateJobInput {
  libraryId: string;
  connectionId: string;
  targetVersion?: string;
  dryRun: boolean;
}

// ---------------- SQL 查询 ----------------

export interface SqlQueryResult {
  columns: string[];
  rows: unknown[][];
  rowCount: number;
  truncated: boolean;
  elapsedMs: number;
}

// ---------------- 脚本库 / 版本登记 / 迁移计划 ----------------

export interface Library {
  id: string;
  name: string;
  createdAt: string;
}

export interface ScriptInfo {
  fileName: string;
  version: string;
  title: string;
  direction: "up" | "down" | "";
  ext: string;
  size: number;
  modifiedAt: string;
}

export interface ScriptContent {
  fileName: string;
  content: string;
}

export interface RegisterVersionInput {
  sourceJobId: string;
  companionJobId?: string;
  includeSchema: boolean;
  includeData: boolean;
  version: string;
  title: string;
  expectedConnectionId: string;
}

export interface RegisterVersionResult {
  upFile: string;
  downFile: string;
}

export interface AppliedVersion {
  version: string;
  title: string;
  status: string;
  checksum: string;
  appliedAt: string;
  executionTime: number;
  errorSummary: string;
}

export interface MigratePlan {
  latestApplied: { version: string; appliedAt: string } | null;
  applied: AppliedVersion[];
  pending: { version: string; title: string }[];
  nextVersion: string;
}

export interface DeleteLedgerRecordInput {
  libraryId: string;
  connectionId: string;
  /** 账本版本号,容忍 V 前缀与大小写 */
  version: string;
  confirmed: true;
}

export interface DeleteLedgerRecordResult {
  ok: boolean;
  /** 实际删除的账本精确版本号 */
  version: string;
  /** 删除前的提示(缺 up 脚本/回退栈序/MySQL 残留) */
  warnings: string[];
}

// ---------------- 下载地址(浏览器直接 <a> 访问) ----------------

export const artifactFileUrl = (jobId: string, name: string): string =>
  `${BASE}/jobs/${enc(jobId)}/artifacts/${enc(name)}`;

export const artifactZipUrl = (jobId: string): string =>
  `${BASE}/jobs/${enc(jobId)}/artifacts/download`;

export const scriptDownloadUrl = (libId: string, fileName: string): string =>
  `${BASE}/libraries/${enc(libId)}/scripts/${enc(fileName)}/download`;

// ---------------- API ----------------

export const api = {
  health: () => request<{ ok: boolean }>("/health"),

  // 连接
  listConnections: () => request<Connection[]>("/connections"),
  createConnection: (body: ConnectionInput) =>
    request<Connection>("/connections", { method: "POST", body: JSON.stringify(body) }),
  updateConnection: (id: string, body: ConnectionInput) =>
    request<Connection>(`/connections/${enc(id)}`, { method: "PUT", body: JSON.stringify(body) }),
  deleteConnection: (id: string) =>
    request<void>(`/connections/${enc(id)}`, { method: "DELETE" }),
  testConnection: (id: string) =>
    request<TestResult>(`/connections/${enc(id)}/test`, { method: "POST" }),
  listTables: (id: string, kind: TableKind = "table") =>
    request<{ tables: TableInfo[] }>(`/connections/${enc(id)}/tables${qs({ kind })}`),

  // 比对方案
  listSchemes: () => request<Scheme[]>("/schemes"),
  createScheme: (body: { name: string; tables: SchemeTable[] }) =>
    request<Scheme>("/schemes", { method: "POST", body: JSON.stringify(body) }),
  updateScheme: (id: string, body: { name: string; tables: SchemeTable[] }) =>
    request<Scheme>(`/schemes/${enc(id)}`, { method: "PUT", body: JSON.stringify(body) }),
  deleteScheme: (id: string) =>
    request<void>(`/schemes/${enc(id)}`, { method: "DELETE" }),

  // 任务
  createJobDiffSchema: (body: DiffSchemaJobInput) =>
    request<{ job: Job }>("/jobs/diff-schema", { method: "POST", body: JSON.stringify(body) }),
  createJobDiffData: (body: DiffDataJobInput) =>
    request<{ job: Job }>("/jobs/diff-data", { method: "POST", body: JSON.stringify(body) }),
  createJobDiffFull: (body: DiffFullJobInput) =>
    request<{ job: Job }>("/jobs/diff-full", { method: "POST", body: JSON.stringify(body) }),
  createJobExecSql: (body: ExecSqlJobInput) =>
    request<{ job: Job }>("/jobs/exec-sql", { method: "POST", body: JSON.stringify(body) }),
  createJobReset: (body: { connectionId: string; confirmed: true }) =>
    request<{ job: Job }>("/jobs/reset", { method: "POST", body: JSON.stringify(body) }),
  createJobMigrate: (body: MigrateJobInput) =>
    request<{ job: Job }>("/jobs/migrate", { method: "POST", body: JSON.stringify(body) }),
  createJobRollback: (body: {
    libraryId: string;
    connectionId: string;
    /** 留空 = 回退最新一个版本;指定 = 从最新依次回退直到该版本 */
    targetVersion?: string;
    confirmed: true;
  }) => request<{ job: Job }>("/jobs/rollback", { method: "POST", body: JSON.stringify(body) }),
  listJobs: (limit = 50) => request<Job[]>(`/jobs${qs({ limit: String(limit) })}`),
  getJob: (id: string) => request<Job>(`/jobs/${enc(id)}`),
  cancelJob: (id: string) =>
    request<{ ok: boolean }>(`/jobs/${enc(id)}/cancel`, { method: "POST" }),
  jobArtifacts: (id: string) => request<Artifact[]>(`/jobs/${enc(id)}/artifacts`),

  // SQL 查询(同步)
  sqlQuery: (body: { connectionId: string; sql: string }) =>
    request<SqlQueryResult>("/sql/query", { method: "POST", body: JSON.stringify(body) }),

  // 重置预览(同步,不连接)
  resetPreview: (body: { connectionId: string }) =>
    request<{ sql: string }>("/reset/preview", { method: "POST", body: JSON.stringify(body) }),

  // 脚本库
  listLibraries: () => request<Library[]>("/libraries"),
  createLibrary: (body: { name: string }) =>
    request<Library>("/libraries", { method: "POST", body: JSON.stringify(body) }),
  deleteLibrary: (id: string) => request<void>(`/libraries/${enc(id)}`, { method: "DELETE" }),
  listScripts: (libId: string) => request<ScriptInfo[]>(`/libraries/${enc(libId)}/scripts`),
  getScript: (libId: string, fileName: string) =>
    request<ScriptContent>(`/libraries/${enc(libId)}/scripts/${enc(fileName)}`),
  putScript: (libId: string, body: ScriptContent) =>
    request<ScriptContent>(`/libraries/${enc(libId)}/scripts/${enc(body.fileName)}`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),
  createScript: (libId: string, body: ScriptContent) =>
    request<ScriptContent>(`/libraries/${enc(libId)}/scripts`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  deleteScript: (libId: string, fileName: string) =>
    request<void>(`/libraries/${enc(libId)}/scripts/${enc(fileName)}`, { method: "DELETE" }),
  copyLibrary: (id: string, body: { name: string }) =>
    request<Library>(`/libraries/${enc(id)}/copy`, { method: "POST", body: JSON.stringify(body) }),
  copyScripts: (libId: string, body: { targetLibraryId: string; fileNames: string[] }) =>
    request<{ copied: number }>(`/libraries/${enc(libId)}/scripts/copy`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  // 增量版本登记与迁移计划
  registerVersion: (libId: string, body: RegisterVersionInput) =>
    request<RegisterVersionResult>(`/libraries/${enc(libId)}/versions`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  migratePlan: (libraryId: string, connectionId: string) =>
    request<MigratePlan>(`/migrate/plan${qs({ libraryId, connectionId })}`),
  deleteMigrateLedgerRecord: (body: DeleteLedgerRecordInput) =>
    request<DeleteLedgerRecordResult>("/migrate/ledger/delete", {
      method: "POST",
      body: JSON.stringify(body),
    }),
};
