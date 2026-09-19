import { SaveOutlined } from "@ant-design/icons";
import {
  Alert,
  App,
  Button,
  Card,
  Col,
  Collapse,
  Input,
  InputNumber,
  Modal,
  Row,
  Select,
  Space,
  Switch,
  Tag,
  Transfer,
  Typography,
} from "antd";
import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  api,
  errMsg,
  type Scheme,
  type SchemeTable,
  type TableInfo,
} from "../api";
import { useConnections } from "../hooks";

interface TableConfig {
  columns: string[];
  ignoreColumns: string[];
}

function TableConfigPanel({
  info,
  config,
  onChange,
}: {
  info: TableInfo | undefined;
  config: TableConfig;
  onChange: (patch: Partial<TableConfig>) => void;
}) {
  const cols = info?.columns ?? [];
  const pk = cols.filter((c) => c.primaryKey).map((c) => c.name);
  const options = cols.map((c) => ({
    value: c.name,
    label: `${c.name}(${c.dataType})${c.primaryKey ? " [PK]" : ""}`,
  }));
  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <div>
        <Typography.Text type="secondary">匹配键(只读,引擎优先主键、回退非空唯一索引):</Typography.Text>
      </div>
      <div>
        {pk.length > 0 ? (
          <Tag color="geekblue">主键:{pk.join(", ")}</Tag>
        ) : (
          <Tag color="red">未检测到主键(接口未返回唯一索引信息,引擎将回退唯一索引或拒绝)</Tag>
        )}
      </div>
      <Row gutter={16}>
        <Col span={12}>
          <Typography.Text>比对列(留空 = 全部列参与)</Typography.Text>
          <Select
            mode="multiple"
            allowClear
            showSearch
            style={{ width: "100%", marginTop: 4 }}
            placeholder="全部列"
            maxTagCount="responsive"
            options={options}
            value={config.columns}
            onChange={(v) => onChange({ columns: v })}
          />
        </Col>
        <Col span={12}>
          <Typography.Text>忽略列(从比对列中剔除)</Typography.Text>
          <Select
            mode="multiple"
            allowClear
            showSearch
            style={{ width: "100%", marginTop: 4 }}
            placeholder="不忽略任何列"
            maxTagCount="responsive"
            options={options}
            value={config.ignoreColumns}
            onChange={(v) => onChange({ ignoreColumns: v })}
          />
        </Col>
      </Row>
    </Space>
  );
}

export default function DiffDataPage() {
  const { message } = App.useApp();
  const navigate = useNavigate();
  const { connections, loading: connsLoading, reload } = useConnections(message.error);
  const [sourceId, setSourceId] = useState<string>();
  const [targetId, setTargetId] = useState<string>();
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [targetKeys, setTargetKeys] = useState<string[]>([]);
  const [configs, setConfigs] = useState<Record<string, TableConfig>>({});
  const [schemes, setSchemes] = useState<Scheme[]>([]);
  const [schemesLoading, setSchemesLoading] = useState(false);
  const [schemeId, setSchemeId] = useState<string | undefined>();
  const [saveOpen, setSaveOpen] = useState(false);
  const [saveName, setSaveName] = useState("");
  const [savingScheme, setSavingScheme] = useState(false);
  const [batchSize, setBatchSize] = useState<number | undefined>();
  const [chunkSize, setChunkSize] = useState<number | undefined>();
  const [dmlBatchSize, setDmlBatchSize] = useState<number | undefined>();
  const [chunkHash, setChunkHash] = useState(false);
  const [bestEffort, setBestEffort] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const loadSchemes = async () => {
    setSchemesLoading(true);
    try {
      setSchemes(await api.listSchemes());
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSchemesLoading(false);
    }
  };

  useEffect(() => {
    void loadSchemes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const connOptions = connections.map((c) => ({
    value: c.id,
    label: `${c.name}(${c.type === "mysql" ? "MySQL" : "PostgreSQL"} · ${c.host}:${c.port}/${c.dbname})`,
  }));

  const transferData = useMemo(() => tables.map((t) => ({ key: t.name, title: t.name })), [tables]);

  const loadTables = async () => {
    if (!sourceId) return;
    setLoadingTables(true);
    try {
      const r = await api.listTables(sourceId, "table");
      setTables(r.tables);
      setTargetKeys([]);
      setConfigs({});
      setSchemeId(undefined);
      message.info(`已从 source 加载 ${r.tables.length} 张表(含列与主键信息)`);
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setLoadingTables(false);
    }
  };

  const applyScheme = (id?: string) => {
    setSchemeId(id);
    if (!id) return;
    const s = schemes.find((x) => x.id === id);
    if (!s) return;
    if (tables.length === 0) {
      message.warning("请先从 source 拉取表清单,再加载方案");
      setSchemeId(undefined);
      return;
    }
    const names = new Set(tables.map((t) => t.name));
    const known = s.tables.filter((t) => names.has(t.table));
    const nextConfigs: Record<string, TableConfig> = {};
    for (const t of known) {
      nextConfigs[t.table] = { columns: t.columns ?? [], ignoreColumns: t.ignoreColumns ?? [] };
    }
    setTargetKeys(known.map((t) => t.table));
    setConfigs(nextConfigs);
    if (known.length < s.tables.length) {
      message.warning(`方案中 ${s.tables.length - known.length} 张表不在当前表清单中,已跳过`);
    } else {
      message.success(`已加载方案「${s.name}」:${known.length} 张表`);
    }
  };

  const updateConfig = (name: string, patch: Partial<TableConfig>) => {
    setConfigs((prev) => ({
      ...prev,
      [name]: {
        columns: prev[name]?.columns ?? [],
        ignoreColumns: prev[name]?.ignoreColumns ?? [],
        ...patch,
      },
    }));
  };

  const buildSchemeTables = (): SchemeTable[] =>
    targetKeys.map((name) => ({
      table: name,
      columns: configs[name]?.columns ?? [],
      ignoreColumns: configs[name]?.ignoreColumns ?? [],
    }));

  const submitSaveScheme = async () => {
    const name = saveName.trim();
    if (!name) {
      message.warning("请输入方案名称");
      return;
    }
    if (targetKeys.length === 0) {
      message.warning("请先选择至少一张表");
      return;
    }
    setSavingScheme(true);
    try {
      const s = await api.createScheme({ name, tables: buildSchemeTables() });
      message.success(`方案「${s.name}」已保存`);
      setSaveOpen(false);
      setSaveName("");
      await loadSchemes();
      setSchemeId(s.id);
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSavingScheme(false);
    }
  };

  const submitJob = async () => {
    if (!sourceId || !targetId) {
      message.warning("请先选择 source 与 target 连接");
      return;
    }
    if (sourceId === targetId) {
      message.error("source 与 target 不能是同一个连接");
      return;
    }
    if (targetKeys.length === 0) {
      message.warning("请至少选择一张要比对的表");
      return;
    }
    setSubmitting(true);
    try {
      await api.createJobDiffData({
        sourceId,
        targetId,
        ...(schemeId ? { schemeId } : {}),
        tables: buildSchemeTables(),
        ...(batchSize ? { batchSize } : {}),
        ...(chunkSize ? { chunkSize } : {}),
        ...(dmlBatchSize ? { dmlBatchSize } : {}),
        chunkHash,
        bestEffort,
      });
      message.success("数据比对任务已提交,正在跳转任务列表");
      navigate("/jobs");
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSubmitting(false);
    }
  };

  const collapseItems = targetKeys.map((name) => {
    const info = tables.find((t) => t.name === name);
    const cfg = configs[name] ?? { columns: [], ignoreColumns: [] };
    return {
      key: name,
      label: (
        <Space wrap size={4}>
          <span>{name}</span>
          {cfg.columns.length > 0 && <Tag color="blue">比对列 {cfg.columns.length}</Tag>}
          {cfg.ignoreColumns.length > 0 && <Tag color="orange">忽略列 {cfg.ignoreColumns.length}</Tag>}
          {!info && <Tag color="red">列信息缺失</Tag>}
        </Space>
      ),
      children: (
        <TableConfigPanel info={info} config={cfg} onChange={(patch) => updateConfig(name, patch)} />
      ),
    };
  });

  return (
    <Card title="数据比对(Data Diff)">
      <Alert
        type="warning"
        showIcon
        style={{ marginBottom: 16 }}
        message="source=将被变更的库,target=参照标准"
        description="行级差异产出的正向/回滚 SQL 均预期在 source 上执行。匹配键=主键(回退非空唯一索引);比对列未指定时为全部列。"
      />
      <Row gutter={16} align="middle">
        <Col span={9}>
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
            onChange={(v) => {
              setSourceId(v);
              setTables([]);
              setTargetKeys([]);
              setConfigs({});
              setSchemeId(undefined);
            }}
          />
        </Col>
        <Col span={9}>
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
        <Col span={6} style={{ paddingTop: 22 }}>
          <Space wrap>
            <Button icon={<SaveOutlined />} disabled={targetKeys.length === 0} onClick={() => setSaveOpen(true)}>
              另存为方案
            </Button>
            <Button onClick={() => void reload()} loading={connsLoading}>
              刷新连接
            </Button>
          </Space>
        </Col>
      </Row>
      <Space direction="vertical" style={{ width: "100%", marginTop: 20 }} size="middle">
        <Space wrap>
          <Typography.Text>比对方案:</Typography.Text>
          <Select
            style={{ width: 260 }}
            placeholder="加载已保存的方案"
            loading={schemesLoading}
            value={schemeId}
            onChange={(v) => applyScheme(v)}
            onClear={() => applyScheme(undefined)}
            allowClear
            options={schemes.map((s) => ({ value: s.id, label: `${s.name}(${s.tables.length} 表)` }))}
          />
          <Button loading={loadingTables} disabled={!sourceId} onClick={() => void loadTables()}>
            从 source 拉取表清单
          </Button>
          <Typography.Text type="secondary">表清单含列与主键信息,用于列勾选与匹配键展示</Typography.Text>
        </Space>
        {transferData.length > 0 && (
          <Transfer
            dataSource={transferData}
            titles={["可选表", "已选表"]}
            targetKeys={targetKeys}
            onChange={(next) => setTargetKeys(next.map(String))}
            render={(item) => item.title}
            showSearch
            listStyle={{ width: 300, height: 340 }}
          />
        )}
        {collapseItems.length > 0 && (
          <Collapse items={collapseItems} defaultActiveKey={collapseItems.slice(0, 1).map((i) => i.key)} />
        )}
        <Collapse
          items={[
            {
              key: "adv",
              label: "高级参数(可选,留空使用引擎默认)",
              children: (
                <Row gutter={16}>
                  <Col span={5}>
                    <Typography.Text type="secondary">批量行数 batchSize</Typography.Text>
                    <InputNumber
                      style={{ width: "100%" }}
                      min={1}
                      value={batchSize}
                      onChange={(v) => setBatchSize(v ?? undefined)}
                      placeholder="引擎默认"
                    />
                  </Col>
                  <Col span={5}>
                    <Typography.Text type="secondary">分块行数 chunkSize</Typography.Text>
                    <InputNumber
                      style={{ width: "100%" }}
                      min={1}
                      value={chunkSize}
                      onChange={(v) => setChunkSize(v ?? undefined)}
                      placeholder="引擎默认"
                    />
                  </Col>
                  <Col span={5}>
                    <Typography.Text type="secondary">DML 批量 dmlBatchSize</Typography.Text>
                    <InputNumber
                      style={{ width: "100%" }}
                      min={1}
                      value={dmlBatchSize}
                      onChange={(v) => setDmlBatchSize(v ?? undefined)}
                      placeholder="引擎默认"
                    />
                  </Col>
                  <Col span={4}>
                    <Typography.Text type="secondary">分块哈希 chunkHash</Typography.Text>
                    <div style={{ marginTop: 6 }}>
                      <Switch checked={chunkHash} onChange={setChunkHash} />
                    </div>
                  </Col>
                  <Col span={5}>
                    <Typography.Text type="secondary">尽力而为 bestEffort</Typography.Text>
                    <div style={{ marginTop: 6 }}>
                      <Switch checked={bestEffort} onChange={setBestEffort} />
                    </div>
                  </Col>
                </Row>
              ),
            },
          ]}
        />
        <div style={{ textAlign: "right" }}>
          <Button type="primary" size="large" loading={submitting} onClick={() => void submitJob()}>
            提交数据比对任务
          </Button>
        </div>
      </Space>
      <Modal
        title="另存为比对方案"
        open={saveOpen}
        onCancel={() => setSaveOpen(false)}
        onOk={() => void submitSaveScheme()}
        confirmLoading={savingScheme}
        okText="保存"
        cancelText="取消"
      >
        <Input
          placeholder="方案名称,如:用户库全量比对"
          value={saveName}
          onChange={(e) => setSaveName(e.target.value)}
        />
        <Typography.Text type="secondary">
          将保存当前已选 {targetKeys.length} 张表及其比对列/忽略列配置。
        </Typography.Text>
      </Modal>
    </Card>
  );
}
