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
  Radio,
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
  type Library,
  type Scheme,
  type SchemeTable,
  type TableInfo,
} from "../api";
import { useConnections } from "../hooks";

interface TableConfig {
  columns: string[];
  ignoreColumns: string[];
}

type Scope = "all" | "include" | "exclude";

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
        {pk.length > 0 ? (
          <Tag color="geekblue">主键:{pk.join(", ")}</Tag>
        ) : (
          <Tag color="red">未检测到主键(引擎将回退唯一索引或拒绝)</Tag>
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

export default function DiffFullPage() {
  const { message } = App.useApp();
  const navigate = useNavigate();
  const { connections, loading: connsLoading, reload } = useConnections(message.error);
  const [sourceId, setSourceId] = useState<string>();
  const [targetId, setTargetId] = useState<string>();
  const [scope, setScope] = useState<Scope>("all");
  const [scopePicked, setScopePicked] = useState<string[]>([]);
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [targetKeys, setTargetKeys] = useState<string[]>([]);
  const [configs, setConfigs] = useState<Record<string, TableConfig>>({});
  const [schemes, setSchemes] = useState<Scheme[]>([]);
  const [schemeId, setSchemeId] = useState<string | undefined>();
  const [batchSize, setBatchSize] = useState<number | undefined>();
  const [chunkSize, setChunkSize] = useState<number | undefined>();
  const [dmlBatchSize, setDmlBatchSize] = useState<number | undefined>();
  const [chunkHash, setChunkHash] = useState(false);
  const [bestEffort, setBestEffort] = useState(false);
  const [register, setRegister] = useState(false);
  const [libs, setLibs] = useState<Library[]>([]);
  const [libId, setLibId] = useState<string | undefined>();
  const [version, setVersion] = useState("");
  const [title, setTitle] = useState("");
  const [expectedConnId, setExpectedConnId] = useState<string | undefined>();
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    api
      .listSchemes()
      .then(setSchemes)
      .catch((e) => message.error(errMsg(e)));
    api
      .listLibraries()
      .then(setLibs)
      .catch((e) => message.error(errMsg(e)));
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

  const submitJob = async () => {
    if (!sourceId || !targetId) {
      message.warning("请先选择 source 与 target 连接");
      return;
    }
    if (sourceId === targetId) {
      message.error("source 与 target 不能是同一个连接");
      return;
    }
    if (scope !== "all" && scopePicked.length === 0) {
      message.warning(scope === "include" ? "请至少选择一张要比对的表" : "请至少选择一张要排除的表");
      return;
    }
    if (targetKeys.length === 0) {
      message.warning("请至少选择一张要比对数据的表");
      return;
    }
    if (register && (!libId || !title.trim())) {
      message.warning("已选择登记为迁移版本:请填写目标脚本库与版本标题");
      return;
    }
    setSubmitting(true);
    try {
      await api.createJobDiffFull({
        sourceId,
        targetId,
        includeTables: scope === "include" ? scopePicked : [],
        excludeTables: scope === "exclude" ? scopePicked : [],
        ...(schemeId ? { schemeId } : {}),
        tables: buildSchemeTables(),
        ...(batchSize ? { batchSize } : {}),
        ...(chunkSize ? { chunkSize } : {}),
        ...(dmlBatchSize ? { dmlBatchSize } : {}),
        chunkHash,
        bestEffort,
        ...(register && libId
          ? {
              register: {
                libraryId: libId,
                ...(version.trim() ? { version: version.trim() } : {}),
                title: title.trim(),
                ...(expectedConnId ? { expectedConnectionId: expectedConnId } : {}),
              },
            }
          : {}),
      });
      message.success("完全比对任务已提交,正在跳转任务列表");
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
    <Card title="完全比对(Full Diff:结构 + 数据)">
      <Alert
        type="warning"
        showIcon
        style={{ marginBottom: 16 }}
        message="一次完成结构比对与数据比对,可勾选在任务成功后一步登记为脚本库迁移版本"
        description="up = 结构正向 → 数据正向;down = 数据回滚 → 结构回滚。仅单侧存在的表由结构比对覆盖,自动跳过其数据比对。登记仅生成 up/down 脚本,执行仍需在「迁移管理」显式进行。"
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
          <Button icon={<SaveOutlined />} disabled={!sourceId} loading={loadingTables} onClick={() => void loadTables()}>
            从 source 拉取表清单
          </Button>
          <Button style={{ marginLeft: 8 }} onClick={() => void reload()} loading={connsLoading}>
            刷新连接
          </Button>
        </Col>
      </Row>
      <Space direction="vertical" style={{ width: "100%", marginTop: 20 }} size="middle">
        <div>
          <Typography.Text strong>结构比对表范围</Typography.Text>
          <Space wrap style={{ marginTop: 4 }}>
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
            {scope !== "all" && (
              <Select
                mode="multiple"
                showSearch
                allowClear
                style={{ minWidth: 320 }}
                placeholder={scope === "include" ? "选择要比对的表" : "选择要排除的表"}
                options={tables.map((t) => ({ value: t.name, label: t.name }))}
                value={scopePicked}
                onChange={(v) => setScopePicked(v)}
                maxTagCount="responsive"
              />
            )}
          </Space>
        </div>
        <Space wrap>
          <Typography.Text>数据比对方案:</Typography.Text>
          <Select
            style={{ width: 260 }}
            allowClear
            placeholder="加载已保存的方案"
            value={schemeId}
            onChange={(v) => applyScheme(v)}
            onClear={() => applyScheme(undefined)}
            options={schemes.map((s) => ({ value: s.id, label: `${s.name}(${s.tables.length} 表)` }))}
          />
          <Typography.Text type="secondary">方案在「数据比对」页维护;表清单含列与主键信息</Typography.Text>
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
            {
              key: "register",
              label: "登记为迁移版本(可选,任务成功后自动执行)",
              children: (
                <Space direction="vertical" style={{ width: "100%" }} size="middle">
                  <Space wrap align="center">
                    <Typography.Text>启用一步登记:</Typography.Text>
                    <Switch checked={register} onChange={setRegister} />
                    <Typography.Text type="secondary">
                      将四个比对产物合成为 V{"{版本}"}__{"{标题}"}.up/down.sql 写入脚本库;版本号留空自动取下一版本
                    </Typography.Text>
                  </Space>
                  {register && (
                    <Row gutter={16}>
                      <Col span={8}>
                        <Typography.Text type="secondary">目标脚本库</Typography.Text>
                        <Select
                          style={{ width: "100%", marginTop: 4 }}
                          placeholder="选择脚本库"
                          value={libId}
                          onChange={(v) => setLibId(v)}
                          options={libs.map((l) => ({ value: l.id, label: l.name }))}
                        />
                      </Col>
                      <Col span={5}>
                        <Typography.Text type="secondary">版本号(留空自动)</Typography.Text>
                        <Input
                          style={{ marginTop: 4 }}
                          value={version}
                          onChange={(e) => setVersion(e.target.value)}
                          placeholder="如 1.0.1"
                        />
                      </Col>
                      <Col span={5}>
                        <Typography.Text type="secondary">标题(不含点号)</Typography.Text>
                        <Input
                          style={{ marginTop: 4 }}
                          value={title}
                          onChange={(e) => setTitle(e.target.value)}
                          placeholder="如 full-sync-user"
                        />
                      </Col>
                      <Col span={6}>
                        <Typography.Text type="secondary">预期执行库(默认 source)</Typography.Text>
                        <Select
                          style={{ width: "100%", marginTop: 4 }}
                          allowClear
                          placeholder="默认取 source 连接"
                          value={expectedConnId}
                          onChange={(v) => setExpectedConnId(v || undefined)}
                          options={connOptions}
                        />
                      </Col>
                    </Row>
                  )}
                </Space>
              ),
            },
          ]}
        />
        <div style={{ textAlign: "right" }}>
          <Button type="primary" size="large" loading={submitting} onClick={() => void submitJob()}>
            提交完全比对任务
          </Button>
        </div>
      </Space>
    </Card>
  );
}
