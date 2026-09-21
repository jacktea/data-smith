import { ExclamationCircleOutlined } from "@ant-design/icons";
import {
  Alert,
  App,
  Button,
  Card,
  Radio,
  Select,
  Space,
  Table,
  Tabs,
  Tag,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  api,
  errMsg,
  type ExecSqlMode,
  type ExecSqlTargetRole,
  type SqlQueryResult,
} from "../api";
import { useConnections } from "../hooks";
import SqlEditor from "../components/SqlEditor";
import { changeSqlConsoleConnection } from "../connectionSelection.js";

const MODE_LABEL: Record<ExecSqlMode, string> = {
  dryrun: "试跑(dry-run)",
  tx: "事务执行(tx)",
  direct: "直接执行(direct)",
};

export default function SqlConsolePage() {
  const { message, modal } = App.useApp();
  const navigate = useNavigate();
  const { connections, loading: connsLoading, reload } = useConnections(message.error);
  const [connId, setConnId] = useState<string>();
  const [tab, setTab] = useState("query");
  const [querySql, setQuerySql] = useState("");
  const [querying, setQuerying] = useState(false);
  const [result, setResult] = useState<SqlQueryResult | null>(null);
  const [scriptSql, setScriptSql] = useState("");
  const [mode, setMode] = useState<ExecSqlMode>("dryrun");
  const [targetRole, setTargetRole] = useState<ExecSqlTargetRole>();
  const [submitting, setSubmitting] = useState(false);

  const selectedConn = connections.find((c) => c.id === connId);
  const dbType = selectedConn?.type as "mysql" | "postgres" | undefined;
  const selectedConnIdentity = selectedConn
    ? `${selectedConn.name} (${selectedConn.host}:${selectedConn.port}/${selectedConn.dbname})`
    : connId;

  const connOptions = connections.map((c) => ({
    value: c.id,
    label: `${c.name}(${c.type === "mysql" ? "MySQL" : "PostgreSQL"} · ${c.host}:${c.port}/${c.dbname})`,
  }));

  const runQuery = async () => {
    if (!connId || !querySql.trim()) return;
    setQuerying(true);
    try {
      setResult(await api.sqlQuery({ connectionId: connId, sql: querySql }));
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setQuerying(false);
    }
  };

  const execScript = async () => {
    if (!connId || !targetRole) return;
    setSubmitting(true);
    try {
      await api.createJobExecSql({ connectionId: connId, content: scriptSql, mode, targetRole });
      message.success("脚本任务已提交,正在跳转任务列表");
      setScriptSql("");
      navigate("/jobs");
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSubmitting(false);
    }
  };

  const confirmScript = () => {
    if (!connId || !targetRole) return;
    modal.confirm({
      title: `确认以「${MODE_LABEL[mode]}」方式执行脚本到 ${targetRole}?`,
      icon: <ExclamationCircleOutlined />,
      content: `物理连接: ${selectedConnIdentity}。脚本将对该连接执行写操作/DDL,请再次核对连接、角色与脚本内容。`,
      okText: "执行",
      okButtonProps: { danger: mode !== "dryrun" },
      cancelText: "取消",
      onOk: () => execScript(),
    });
  };

  const resultColumns: ColumnsType<Record<string, unknown>> = (result?.columns ?? []).map((c, i) => ({
    title: c,
    dataIndex: String(i),
    key: `${c}-${i}`,
    ellipsis: true,
    render: (v: unknown) => (v === null || v === undefined ? "-" : String(v)),
  }));
  const resultRows = (result?.rows ?? []).map((r, i) => {
    const o: Record<string, unknown> = { __key: i };
    r.forEach((v, j) => {
      o[String(j)] = v;
    });
    return o;
  });

  const queryTab = {
    key: "query",
    label: "查询(只读)",
    children: (
      <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
        <div style={{ flexShrink: 0, marginBottom: 12 }}>
          <SqlEditor
            value={querySql}
            onChange={setQuerySql}
            dialect={dbType}
            height="140px"
            placeholder={
              "仅允许单条只读语句: SELECT / WITH / SHOW / EXPLAIN (MySQL 亦支持 DESC / DESCRIBE)\n例: SELECT * FROM users ORDER BY id LIMIT 20\n按 Cmd+Enter (Ctrl+Enter) 可直接执行查询"
            }
            onExecute={() => {
              if (connId && querySql.trim() && !querying) {
                void runQuery();
              }
            }}
          />
          <Space style={{ marginTop: 10 }} wrap align="center">
            <Button
              type="primary"
              loading={querying}
              disabled={!connId || !querySql.trim()}
              onClick={() => void runQuery()}
            >
              执行查询
            </Button>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              快捷键: Cmd+Enter / Ctrl+Enter
            </Typography.Text>
            {result && (
              <>
                <Tag color="blue">{result.rowCount} 行</Tag>
                <Tag>{result.elapsedMs} ms</Tag>
                {result.truncated && <Tag color="orange">已达 1000 行上限,结果被截断</Tag>}
              </>
            )}
          </Space>
        </div>
        <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
          {!result ? (
            <div className="ds-sql-empty-result">
              暂无查询结果，请输入 SQL 并点击「执行查询」或按 Cmd/Ctrl+Enter
            </div>
          ) : result.columns.length === 0 ? (
            <Typography.Text type="secondary" style={{ padding: 12 }}>
              查询执行完成,无结果列。
            </Typography.Text>
          ) : (
            <Table
              className="ds-sql-result-table"
              size="small"
              rowKey="__key"
              columns={resultColumns}
              dataSource={resultRows}
              scroll={{ x: "max-content", y: "100%" }}
              pagination={{ pageSize: 50, showSizeChanger: false, showTotal: (n) => `共 ${n} 行` }}
            />
          )}
        </div>
      </div>
    ),
  };

  const scriptTab = {
    key: "script",
    label: "脚本(写入)",
    children: (
      <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 12, flexShrink: 0 }}
          message="危险操作:脚本将直接写入所选连接对应的库"
          description="三态说明 —— 试跑(dry-run):事务中执行后回滚,仅做验证(MySQL DDL 不支持试跑,将被引擎拒绝);事务执行(tx):成功提交、失败整体回滚;直接执行(direct):逐条提交,失败不回滚。引擎会校验脚本头部 EXECUTE-ON 标记与所选库的一致性,不一致即拒绝执行。"
        />
        <div style={{ marginBottom: 12, flexShrink: 0 }}>
          <Space wrap>
            <Typography.Text strong>连接角色:</Typography.Text>
            <Radio.Group
              value={targetRole}
              onChange={(e) => setTargetRole(e.target.value as ExecSqlTargetRole)}
              optionType="button"
              buttonStyle="solid"
              options={[
                { value: "source", label: "source" },
                { value: "target", label: "target" },
              ]}
            />
            <Typography.Text type="secondary">
              必须与脚本头部 DATASMITH EXECUTE-ON 标记一致。
            </Typography.Text>
          </Space>
        </div>
        <div style={{ marginBottom: 12, flexShrink: 0 }}>
          <Radio.Group
            value={mode}
            onChange={(e) => setMode(e.target.value as ExecSqlMode)}
            optionType="button"
            buttonStyle="solid"
            options={[
              { value: "dryrun", label: "试跑(dry-run)" },
              { value: "tx", label: "事务执行(tx)" },
              { value: "direct", label: "直接执行(direct)" },
            ]}
          />
        </div>
        <div style={{ minHeight: 0, display: "flex", flexDirection: "column" }}>
          <SqlEditor
            value={scriptSql}
            onChange={setScriptSql}
            dialect={dbType}
            minHeight="220px"
            maxHeight="480px"
            placeholder={
              "可包含多条语句,支持 -- 注释。\n按 Cmd+Enter (Ctrl+Enter) 可快速触发确认并提交。\n将以任务方式异步执行,请到「任务列表」查看日志与产物。"
            }
            onExecute={() => {
              if (connId && targetRole && scriptSql.trim() && !submitting) {
                confirmScript();
              }
            }}
          />
        </div>
        <Space style={{ marginTop: 12, flexShrink: 0 }} align="center">
          <Button
            type="primary"
            danger={mode !== "dryrun"}
            loading={submitting}
            disabled={!connId || !targetRole || !scriptSql.trim()}
            onClick={confirmScript}
          >
            执行脚本
          </Button>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            快捷键: Cmd+Enter / Ctrl+Enter
          </Typography.Text>
          {mode === "direct" && (
            <Typography.Text type="danger" strong>
              直接执行失败不会回滚,请务必确认!
            </Typography.Text>
          )}
        </Space>
      </div>
    ),
  };

  return (
    <Card title="SQL 控制台" className="ds-tab-page-card">
      <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
        <div style={{ flexShrink: 0, marginBottom: 16 }}>
          <Space wrap>
            <Typography.Text strong>连接:</Typography.Text>
            <Select
              showSearch
              optionFilterProp="label"
              style={{ width: 340 }}
              placeholder="选择连接"
              loading={connsLoading}
              options={connOptions}
              value={connId}
              onChange={(v) => {
                const next = changeSqlConsoleConnection(
                  { connId, targetRole, result },
                  v,
                );
                setConnId(next.connId);
                setTargetRole(next.targetRole);
                setResult(next.result);
              }}
            />
            {connId && (
              <Typography.Text type="secondary">
                所选连接是物理执行目标；执行写入脚本时还必须显式选择其 source/target 角色。
              </Typography.Text>
            )}
            <Button size="small" onClick={() => void reload()} loading={connsLoading}>
              刷新
            </Button>
          </Space>
        </div>
        <Tabs
          className="ds-fill-tabs ds-sql-console-tabs"
          activeKey={tab}
          onChange={setTab}
          destroyInactiveTabPane
          items={[queryTab, scriptTab]}
        />
      </div>
    </Card>
  );
}
