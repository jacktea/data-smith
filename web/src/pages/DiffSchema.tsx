import { ReloadOutlined } from "@ant-design/icons";
import { Alert, App, Button, Card, Col, Radio, Row, Select, Space, Typography } from "antd";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api, errMsg } from "../api";
import { useConnections } from "../hooks";

type Scope = "all" | "include" | "exclude";

export default function DiffSchemaPage() {
  const { message } = App.useApp();
  const navigate = useNavigate();
  const { connections, loading: connsLoading, reload } = useConnections(message.error);
  const [sourceId, setSourceId] = useState<string>();
  const [targetId, setTargetId] = useState<string>();
  const [scope, setScope] = useState<Scope>("all");
  const [tableNames, setTableNames] = useState<string[]>([]);
  const [picked, setPicked] = useState<string[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const connOptions = connections.map((c) => ({
    value: c.id,
    label: `${c.name}(${c.type === "mysql" ? "MySQL" : "PostgreSQL"} · ${c.host}:${c.port}/${c.dbname})`,
  }));

  const loadTables = async () => {
    if (!sourceId) return;
    setLoadingTables(true);
    try {
      const r = await api.listTables(sourceId, "table");
      setTableNames(r.tables.map((t) => t.name));
      setPicked([]);
      message.info(`已从 source 加载 ${r.tables.length} 张表`);
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setLoadingTables(false);
    }
  };

  const submit = async () => {
    if (!sourceId || !targetId) {
      message.warning("请先选择 source 与 target 连接");
      return;
    }
    if (sourceId === targetId) {
      message.error("source 与 target 不能是同一个连接");
      return;
    }
    if (scope !== "all" && picked.length === 0) {
      message.warning(scope === "include" ? "请至少选择一张要比对的表" : "请至少选择一张要排除的表");
      return;
    }
    setSubmitting(true);
    try {
      await api.createJobDiffSchema({
        sourceId,
        targetId,
        includeTables: scope === "include" ? picked : [],
        excludeTables: scope === "exclude" ? picked : [],
      });
      message.success("结构比对任务已提交,正在跳转任务列表");
      navigate("/jobs");
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Card title="结构比对(Schema Diff)">
      <Alert
        type="warning"
        showIcon
        style={{ marginBottom: 16 }}
        message="source=将被变更的库,target=参照标准"
        description="正向 SQL 将以 source 方言生成并预期在 source 上执行,使其对齐 target;回滚 SQL 用于逆变换。两类脚本均带 EXECUTE-ON: source 标记。请再三确认两个连接的选择。"
      />
      <Row gutter={16}>
        <Col span={10}>
          <Typography.Text type="danger" strong>
            Source(将被变更的库)
          </Typography.Text>
          <Select
            showSearch
            optionFilterProp="label"
            style={{ width: "100%", marginTop: 4 }}
            placeholder="选择 source 连接"
            loading={connsLoading}
            options={connOptions}
            value={sourceId}
            onChange={(v) => setSourceId(v)}
          />
        </Col>
        <Col span={10}>
          <Typography.Text strong>Target(参照标准)</Typography.Text>
          <Select
            showSearch
            optionFilterProp="label"
            style={{ width: "100%", marginTop: 4 }}
            placeholder="选择 target 连接"
            loading={connsLoading}
            options={connOptions}
            value={targetId}
            onChange={(v) => setTargetId(v)}
          />
        </Col>
        <Col span={4} style={{ paddingTop: 22 }}>
          <Button icon={<ReloadOutlined />} loading={connsLoading} onClick={() => void reload()}>
            刷新连接
          </Button>
        </Col>
      </Row>
      <Space direction="vertical" style={{ width: "100%", marginTop: 24 }} size="middle">
        <Typography.Title level={5} style={{ marginBottom: 0 }}>
          表范围
        </Typography.Title>
        <Space wrap>
          <Radio.Group
            value={scope}
            onChange={(e) => setScope(e.target.value as Scope)}
            optionType="button"
            buttonStyle="solid"
            options={[
              { value: "all", label: "全部表" },
              { value: "include", label: "仅比对选中表" },
              { value: "exclude", label: "排除选中表" },
            ]}
          />
          <Button loading={loadingTables} disabled={!sourceId} onClick={() => void loadTables()}>
            从 source 拉取表清单
          </Button>
          {tableNames.length > 0 && (
            <Typography.Text type="secondary">共 {tableNames.length} 张表</Typography.Text>
          )}
        </Space>
        {scope !== "all" && (
          <Select
            mode="multiple"
            showSearch
            allowClear
            style={{ width: "100%" }}
            placeholder={scope === "include" ? "选择要比对的表" : "选择要排除的表"}
            options={tableNames.map((n) => ({ value: n, label: n }))}
            value={picked}
            onChange={(v) => setPicked(v)}
            maxTagCount="responsive"
          />
        )}
        <div style={{ textAlign: "right" }}>
          <Button type="primary" size="large" loading={submitting} onClick={() => void submit()}>
            提交结构比对任务
          </Button>
        </div>
      </Space>
    </Card>
  );
}
