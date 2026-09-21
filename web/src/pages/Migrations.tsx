import {
  DeleteOutlined,
  DownloadOutlined,
  EditOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  RollbackOutlined,
  UploadOutlined,
} from "@ant-design/icons";
import {
  Alert,
  App,
  Button,
  Card,
  Descriptions,
  Empty,
  Input,
  Modal,
  Popconfirm,
  Select,
  Space,
  Switch,
  Table,
  Tabs,
  Tag,
  Typography,
  Upload,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  api,
  errMsg,
  type Library,
  type MigratePlan,
  type ScriptInfo,
} from "../api";
import { fmtSize, fmtTime } from "../format";
import { useConnections } from "../hooks";
import SqlEditor from "../components/SqlEditor";

// 与引擎解析正则一致:版本__标题[.up|.down].sql|json
const SCRIPT_NAME_RE = /^([vV]\d+(?:\.\d+)*|\d+)__([^.]+)(?:\.(up|down))?\.(sql|json)$/;

// ---------------- Tab 1:脚本库管理 ----------------

function LibraryTab() {
  const { message } = App.useApp();
  const [libs, setLibs] = useState<Library[]>([]);
  const [libId, setLibId] = useState<string>();
  const [scripts, setScripts] = useState<ScriptInfo[]>([]);
  const [loadingScripts, setLoadingScripts] = useState(false);
  const [newLibOpen, setNewLibOpen] = useState(false);
  const [newLibName, setNewLibName] = useState("");
  const [editor, setEditor] = useState<{ fileName: string; content: string } | null>(null);
  const [savingEditor, setSavingEditor] = useState(false);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [upload, setUpload] = useState({ fileName: "", content: "" });
  const [savingUpload, setSavingUpload] = useState(false);

  const loadLibs = useCallback(async () => {
    try {
      const r = await api.listLibraries();
      setLibs(r);
      setLibId((cur) => (cur && r.some((l) => l.id === cur) ? cur : r[0]?.id));
    } catch (e) {
      message.error(errMsg(e));
    }
  }, [message]);

  useEffect(() => {
    void loadLibs();
  }, [loadLibs]);

  const refreshScripts = useCallback(async () => {
    if (!libId) {
      setScripts([]);
      return;
    }
    setLoadingScripts(true);
    try {
      setScripts(await api.listScripts(libId));
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setLoadingScripts(false);
    }
  }, [libId, message]);

  useEffect(() => {
    void refreshScripts();
  }, [refreshScripts]);

  const createLib = async () => {
    const name = newLibName.trim();
    if (!name) {
      message.warning("请输入脚本库名称");
      return;
    }
    try {
      const l = await api.createLibrary({ name });
      message.success(`脚本库「${l.name}」已创建`);
      setNewLibOpen(false);
      setNewLibName("");
      await loadLibs();
      setLibId(l.id);
    } catch (e) {
      message.error(errMsg(e));
    }
  };

  const removeLib = async () => {
    if (!libId) return;
    try {
      await api.deleteLibrary(libId);
      message.success("脚本库已删除(连带脚本)");
      setLibId(undefined);
      await loadLibs();
    } catch (e) {
      message.error(errMsg(e));
    }
  };

  const openEditor = async (f: ScriptInfo) => {
    if (!libId) return;
    try {
      const c = await api.getScript(libId, f.fileName);
      setEditor({ fileName: c.fileName, content: c.content });
    } catch (e) {
      message.error(errMsg(e));
    }
  };

  const saveEditor = async () => {
    if (!editor || !libId) return;
    setSavingEditor(true);
    try {
      await api.putScript(libId, { fileName: editor.fileName, content: editor.content });
      message.success("脚本已保存");
      setEditor(null);
      await refreshScripts();
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSavingEditor(false);
    }
  };

  const submitUpload = async () => {
    if (!libId) return;
    const name = upload.fileName.trim();
    if (!SCRIPT_NAME_RE.test(name)) {
      message.error("文件名需符合 V版本__标题[.up|.down].sql/json,例:V1.0.0__init.up.sql");
      return;
    }
    if (name.includes(".down.")) {
      message.error("down 脚本仅能由「版本登记」生成,不能手动上传");
      return;
    }
    if (name.endsWith(".json")) {
      message.error("手动上传仅支持 .sql 脚本;.json 由服务端生成");
      return;
    }
    if (!upload.content.trim()) {
      message.warning("脚本内容不能为空");
      return;
    }
    setSavingUpload(true);
    try {
      await api.createScript(libId, { fileName: name, content: upload.content });
      message.success("脚本已上传");
      setUploadOpen(false);
      setUpload({ fileName: "", content: "" });
      await refreshScripts();
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSavingUpload(false);
    }
  };

  const removeScript = async (f: ScriptInfo) => {
    if (!libId) return;
    try {
      await api.deleteScript(libId, f.fileName);
      message.success("脚本已删除");
      await refreshScripts();
    } catch (e) {
      message.error(errMsg(e));
    }
  };

  const downloadHref = (fileName: string) =>
    `/api/libraries/${libId}/scripts/${encodeURIComponent(fileName)}/download`;

  const columns: ColumnsType<ScriptInfo> = [
    { title: "文件名", dataIndex: "fileName", render: (v: string) => <Typography.Text code>{v}</Typography.Text> },
    { title: "版本", dataIndex: "version", width: 90 },
    { title: "标题", dataIndex: "title", ellipsis: true },
    {
      title: "方向",
      dataIndex: "direction",
      width: 90,
      render: (d: ScriptInfo["direction"]) =>
        d === "up" ? <Tag color="green">up</Tag> : d === "down" ? <Tag color="red">down</Tag> : <Tag>无方向</Tag>,
    },
    { title: "类型", dataIndex: "ext", width: 70 },
    { title: "大小", dataIndex: "size", width: 90, render: (v: number) => fmtSize(v) },
    { title: "修改时间", dataIndex: "modifiedAt", width: 160, render: (v: string) => fmtTime(v) },
    {
      title: "操作",
      key: "actions",
      width: 240,
      render: (_, f) => (
        <Space size="small">
          <Button size="small" icon={<EditOutlined />} onClick={() => void openEditor(f)}>
            编辑
          </Button>
          <Button size="small" icon={<DownloadOutlined />} href={downloadHref(f.fileName)}>
            下载
          </Button>
          <Popconfirm
            title="确认删除该脚本?"
            okButtonProps={{ danger: true }}
            onConfirm={() => void removeScript(f)}
          >
            <Button size="small" danger icon={<DeleteOutlined />}>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <Space wrap>
        <Select
          style={{ width: 260 }}
          placeholder="选择脚本库"
          loading={libs.length === 0 && !libId}
          value={libId}
          onChange={(v) => setLibId(v)}
          options={libs.map((l) => ({ value: l.id, label: l.name }))}
        />
        <Button icon={<PlusOutlined />} onClick={() => setNewLibOpen(true)}>
          新建脚本库
        </Button>
        <Popconfirm
          title="删除脚本库?"
          description="将连带删除库内全部脚本,不可恢复。"
          okText="删除"
          okButtonProps={{ danger: true }}
          onConfirm={() => void removeLib()}
          disabled={!libId}
        >
          <Button danger disabled={!libId} icon={<DeleteOutlined />}>
            删除脚本库
          </Button>
        </Popconfirm>
        <Button
          type="primary"
          icon={<UploadOutlined />}
          disabled={!libId}
          onClick={() => setUploadOpen(true)}
        >
          上传脚本
        </Button>
      </Space>
      {libId ? (
        <Table
          rowKey="fileName"
          size="small"
          columns={columns}
          dataSource={scripts}
          loading={loadingScripts}
          pagination={false}
        />
      ) : (
        <Empty description="请选择或新建脚本库" />
      )}
      <Modal
        title="新建脚本库"
        open={newLibOpen}
        onCancel={() => setNewLibOpen(false)}
        onOk={() => void createLib()}
        okText="创建"
        cancelText="取消"
      >
        <Input
          placeholder="脚本库名称,如:order-service 迁移"
          value={newLibName}
          onChange={(e) => setNewLibName(e.target.value)}
        />
      </Modal>
      <Modal
        title={`编辑脚本:${editor?.fileName ?? ""}`}
        open={!!editor}
        width={760}
        onCancel={() => setEditor(null)}
        onOk={() => void saveEditor()}
        confirmLoading={savingEditor}
        okText="保存"
        cancelText="取消"
      >
        <SqlEditor
          value={editor?.content ?? ""}
          onChange={(v) => setEditor((prev) => (prev ? { ...prev, content: v } : prev))}
          height="420px"
          toolbar
          onExecute={() => {
            if (editor && !savingEditor) {
              void saveEditor();
            }
          }}
        />
      </Modal>
      <Modal
        title="上传脚本"
        open={uploadOpen}
        width={720}
        onCancel={() => setUploadOpen(false)}
        onOk={() => void submitUpload()}
        confirmLoading={savingUpload}
        okText="上传"
        cancelText="取消"
      >
        <Space direction="vertical" style={{ width: "100%" }} size="middle">
          <Upload
            maxCount={1}
            beforeUpload={(file) => {
              void file.text().then((text) => setUpload({ fileName: file.name, content: text }));
              return false;
            }}
            onRemove={() => setUpload({ fileName: "", content: "" })}
          >
            <Button icon={<UploadOutlined />}>选择本地脚本文件(自动填充文件名与内容)</Button>
          </Upload>
          <Input
            placeholder="文件名,例:V1.0.0__init.up.sql(仅支持 .sql)"
            value={upload.fileName}
            onChange={(e) => setUpload((u) => ({ ...u, fileName: e.target.value }))}
          />
          <SqlEditor
            value={upload.content}
            onChange={(v) => setUpload((u) => ({ ...u, content: v }))}
            height="260px"
            toolbar
            placeholder="脚本内容(SQL);文件名带 .down 或 .json 将被拒绝 —— down/JSON 仅能由版本登记生成"
            onExecute={() => {
              if (libId && !savingUpload) {
                void submitUpload();
              }
            }}
          />
        </Space>
      </Modal>
    </Space>
  );
}

// ---------------- Tab 2:执行计划 ----------------

function statusTag(s: string) {
  if (s === "success") return <Tag color="success">成功</Tag>;
  if (s === "failed") return <Tag color="error">失败</Tag>;
  if (s === "rolled_back") return <Tag color="orange">已回退</Tag>;
  return <Tag>{s}</Tag>;
}

function RunTab() {
  const { message, modal } = App.useApp();
  const navigate = useNavigate();
  const { connections, loading: connsLoading, reload } = useConnections(message.error);
  const [libs, setLibs] = useState<Library[]>([]);
  const [libId, setLibId] = useState<string>();
  const [connId, setConnId] = useState<string>();
  const [plan, setPlan] = useState<MigratePlan | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [targetVersion, setTargetVersion] = useState("");
  const [dryRun, setDryRun] = useState(false);
  const [executing, setExecuting] = useState(false);

  useEffect(() => {
    api
      .listLibraries()
      .then(setLibs)
      .catch((e) => message.error(errMsg(e)));
  }, [message]);

  const connOptions = connections.map((c) => ({
    value: c.id,
    label: `${c.name}(${c.type === "mysql" ? "MySQL" : "PostgreSQL"} · ${c.host}:${c.port}/${c.dbname})`,
  }));
  const selectedLib = libs.find((l) => l.id === libId);

  const loadPlan = async () => {
    if (!libId || !connId) {
      message.warning("请先选择脚本库与连接");
      return;
    }
    setPlanLoading(true);
    try {
      setPlan(await api.migratePlan(libId, connId));
    } catch (e) {
      setPlan(null);
      message.error(errMsg(e));
    } finally {
      setPlanLoading(false);
    }
  };

  const doExec = async () => {
    if (!libId || !connId) return;
    setExecuting(true);
    try {
      await api.createJobMigrate({
        libraryId: libId,
        connectionId: connId,
        ...(targetVersion.trim() ? { targetVersion: targetVersion.trim() } : {}),
        dryRun,
      });
      message.success("迁移任务已提交,正在跳转任务列表");
      navigate("/jobs");
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setExecuting(false);
    }
  };

  const confirmExec = () => {
    if (!plan) return;
    modal.confirm({
      title: dryRun ? "确认试跑迁移?" : "确认执行迁移?",
      content: `将把「${selectedLib?.name ?? "脚本库"}」应用到所选连接${
        targetVersion.trim() ? `,目标版本 ${targetVersion.trim()}` : ",直到最新版本"
      }。${
        dryRun
          ? "试跑在事务中执行后回滚,不会真正变更(MySQL DDL 不支持试跑,将被拒绝)。"
          : "该操作会真实变更数据库结构/数据,请确认!"
      }`,
      okText: dryRun ? "开始试跑" : "开始执行",
      okButtonProps: { danger: !dryRun },
      cancelText: "取消",
      onOk: () => doExec(),
    });
  };

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <Space wrap>
        <Select
          style={{ width: 240 }}
          placeholder="选择脚本库"
          value={libId}
          onChange={(v) => {
            setLibId(v);
            setPlan(null);
          }}
          options={libs.map((l) => ({ value: l.id, label: l.name }))}
        />
        <Select
          style={{ width: 300 }}
          showSearch
          optionFilterProp="label"
          placeholder="选择目标连接(source)"
          loading={connsLoading}
          value={connId}
          onChange={(v) => {
            setConnId(v);
            setPlan(null);
          }}
          options={connOptions}
        />
        <Button type="primary" loading={planLoading} onClick={() => void loadPlan()}>
          查看执行计划
        </Button>
        <Button size="small" onClick={() => void reload()} loading={connsLoading}>
          刷新连接
        </Button>
      </Space>
      {plan && (
        <>
          <Descriptions
            bordered
            size="small"
            column={3}
            items={[
              { key: "lib", label: "脚本库", children: selectedLib?.name ?? "-" },
              {
                key: "latest",
                label: "最新已应用版本",
                children: plan.latestApplied ? (
                  <>
                    {plan.latestApplied.version}({fmtTime(plan.latestApplied.appliedAt)})
                  </>
                ) : (
                  <Tag color="orange">空库,尚无已应用版本</Tag>
                ),
              },
              { key: "next", label: "下一版本", children: <Tag color="geekblue">{plan.nextVersion}</Tag> },
            ]}
          />
          <Typography.Title level={5} style={{ marginBottom: 0 }}>
            已应用版本(账本)
          </Typography.Title>
          <Table
            rowKey="version"
            size="small"
            pagination={false}
            dataSource={plan.applied}
            locale={{ emptyText: "无已应用版本" }}
            columns={[
              { title: "版本", dataIndex: "version", width: 100 },
              { title: "标题", dataIndex: "title", ellipsis: true },
              { title: "状态", dataIndex: "status", width: 100, render: (s: string) => statusTag(s) },
              {
                title: "校验和",
                dataIndex: "checksum",
                width: 140,
                render: (v: string) => (
                  <Typography.Text code style={{ fontSize: 12 }}>
                    {v ? `${v.slice(0, 12)}…` : "-"}
                  </Typography.Text>
                ),
              },
              { title: "应用时间", dataIndex: "appliedAt", width: 160, render: (v: string) => fmtTime(v) },
              {
                title: "耗时",
                dataIndex: "executionTime",
                width: 90,
                render: (v: number) => (v ? `${v} ms` : "-"),
              },
              {
                title: "错误摘要",
                dataIndex: "errorSummary",
                ellipsis: true,
                render: (v: string) => (v ? <Typography.Text type="danger">{v}</Typography.Text> : "-"),
              },
            ]}
          />
          <Typography.Title level={5} style={{ marginBottom: 0 }}>
            待执行版本
          </Typography.Title>
          {plan.pending.length === 0 ? (
            <Alert type="success" showIcon message="库已最新,无待执行版本。" />
          ) : (
            <Table
              rowKey="version"
              size="small"
              pagination={false}
              dataSource={plan.pending}
              columns={[
                { title: "版本", dataIndex: "version", width: 100 },
                { title: "标题", dataIndex: "title", ellipsis: true },
              ]}
            />
          )}
          <Card size="small" title="执行设置">
            <Space direction="vertical" style={{ width: "100%" }} size="middle">
              <Space wrap align="center">
                <Typography.Text>目标版本(留空=执行到最新):</Typography.Text>
                <Input
                  style={{ width: 200 }}
                  value={targetVersion}
                  onChange={(e) => setTargetVersion(e.target.value)}
                  placeholder={plan.nextVersion ? `如 ${plan.nextVersion}` : "如 1.0.0"}
                />
                <Switch checked={dryRun} onChange={setDryRun} checkedChildren="试跑" unCheckedChildren="正式" />
                <Typography.Text type="secondary">
                  试跑=事务中执行后回滚验证;MySQL DDL 不支持试跑(引擎拒绝)
                </Typography.Text>
              </Space>
              <Button
                type="primary"
                danger={!dryRun}
                icon={<PlayCircleOutlined />}
                loading={executing}
                disabled={plan.pending.length === 0}
                onClick={confirmExec}
              >
                {dryRun ? "试跑迁移" : "执行迁移"}
              </Button>
            </Space>
          </Card>
        </>
      )}
      {!plan && <Empty description="选择脚本库与连接后查看执行计划" />}
    </Space>
  );
}

// ---------------- Tab 3:回退 ----------------

function RollbackTab() {
  const { message, modal } = App.useApp();
  const navigate = useNavigate();
  const { connections, loading: connsLoading, reload } = useConnections(message.error);
  const [libs, setLibs] = useState<Library[]>([]);
  const [libId, setLibId] = useState<string>();
  const [connId, setConnId] = useState<string>();
  const [plan, setPlan] = useState<MigratePlan | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [targetVersion, setTargetVersion] = useState<string>();
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    api
      .listLibraries()
      .then(setLibs)
      .catch((e) => message.error(errMsg(e)));
  }, [message]);

  const connOptions = connections.map((c) => ({
    value: c.id,
    label: `${c.name}(${c.type === "mysql" ? "MySQL" : "PostgreSQL"} · ${c.host}:${c.port}/${c.dbname})`,
  }));
  const selectedLib = libs.find((l) => l.id === libId);

  const loadPlan = useCallback(async () => {
    if (!libId || !connId) {
      setPlan(null);
      return;
    }
    setPlanLoading(true);
    try {
      setPlan(await api.migratePlan(libId, connId));
    } catch (e) {
      setPlan(null);
      message.error(errMsg(e));
    } finally {
      setPlanLoading(false);
    }
  }, [libId, connId, message]);

  // 选定脚本库+连接后自动拉取账本,供目标版本下拉与回退预览使用
  useEffect(() => {
    void loadPlan();
  }, [loadPlan]);

  // 回退栈:按应用顺序(账本 id)取成功版本,最新在前
  const successStack = useMemo(
    () =>
      (plan?.applied ?? [])
        .filter((r) => r.status === "success")
        .map((r) => r.version)
        .reverse(),
    [plan],
  );
  const norm = (v: string) => v.trim().toLowerCase().replace(/^v/, "");
  // 将被回退的版本:目标版本之上的全部成功版本;未选目标 = 仅最新一个
  const plannedVersions = useMemo(() => {
    if (!targetVersion) return successStack.slice(0, 1);
    const idx = successStack.findIndex((v) => norm(v) === norm(targetVersion));
    return idx < 0 ? [] : successStack.slice(0, idx);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [successStack, targetVersion]);

  const targetOptions = successStack.map((v) => {
    const row = plan?.applied.find((r) => r.version === v);
    return { value: v, label: `${v}${row?.title ? ` ${row.title}` : ""}` };
  });
  const targetMissing = !!targetVersion && plannedVersions.length === 0 && norm(plannedVersions[0] ?? "") !== norm(targetVersion);

  const doRollback = async () => {
    if (!libId || !connId) return;
    setSubmitting(true);
    try {
      await api.createJobRollback({
        libraryId: libId,
        connectionId: connId,
        ...(targetVersion ? { targetVersion } : {}),
        confirmed: true,
      });
      message.success("回退任务已提交,正在跳转任务列表");
      navigate("/jobs");
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSubmitting(false);
    }
  };

  // 双确认:两段 Modal 依次弹窗
  const confirmRollback = () => {
    if (!libId || !connId) {
      message.warning("请先选择脚本库与连接");
      return;
    }
    if (targetMissing) {
      message.error(`目标版本 ${targetVersion} 不在已成功应用的版本栈中`);
      return;
    }
    const plannedText =
      plannedVersions.length > 0 ? plannedVersions.join(" → ") : `已处于目标版本,无需回退`;
    modal.confirm({
      title: "回退确认(1/2)",
      icon: <RollbackOutlined />,
      content: `将把「${selectedLib?.name ?? "脚本库"}」在所选连接上依次回退 ${
        plannedVersions.length > 0 ? `${plannedVersions.length} 个版本` : ""
      }${targetVersion ? `,直到版本 ${targetVersion}` : "(最新一个版本)"}:
${plannedText}。每个版本执行其 down 脚本并把账本标记为 rolled_back。`,
      okText: plannedVersions.length > 0 ? "下一步" : "知道了",
      cancelText: "取消",
      onOk: () => {
        if (plannedVersions.length === 0) return;
        modal.confirm({
          title: "回退确认(2/2):该操作将变更数据库",
          icon: <RollbackOutlined />,
          content:
            "down 脚本可能删除/改写结构或数据;PG 每步在单事务中执行,MySQL 直接执行。若栈中任一版本缺少 down 脚本,服务端将在执行前拒绝。确认继续?",
          okText: "确认回退",
          okButtonProps: { danger: true },
          cancelText: "取消",
          onOk: () => doRollback(),
        });
      },
    });
  };

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <Alert
        type="warning"
        showIcon
        message="回退最新一个版本,或回退到指定版本(从最新依次回退,栈式)"
        description="每个被回退版本执行其 down 脚本并将账本置为 rolled_back;PG 每步单事务执行(失败整体回滚),MySQL 直接执行。栈中任一版本缺少 down 脚本时,服务端在动库前拒绝。"
      />
      <Space wrap>
        <Select
          style={{ width: 240 }}
          placeholder="选择脚本库"
          value={libId}
          onChange={(v) => {
            setLibId(v);
            setTargetVersion(undefined);
          }}
          options={libs.map((l) => ({ value: l.id, label: l.name }))}
        />
        <Select
          style={{ width: 300 }}
          showSearch
          optionFilterProp="label"
          placeholder="选择目标连接(source)"
          loading={connsLoading}
          value={connId}
          onChange={(v) => {
            setConnId(v);
            setTargetVersion(undefined);
          }}
          options={connOptions}
        />
        <Select
          style={{ width: 280 }}
          showSearch
          optionFilterProp="label"
          allowClear
          placeholder="回退目标(留空 = 仅最新一个)"
          loading={planLoading}
          value={targetVersion}
          onChange={(v) => setTargetVersion(v || undefined)}
          options={targetOptions}
        />
        <Button size="small" onClick={() => void reload()} loading={connsLoading}>
          刷新连接
        </Button>
        <Button
          type="primary"
          danger={plannedVersions.length > 0}
          icon={<RollbackOutlined />}
          loading={submitting}
          disabled={!!connId && targetMissing}
          onClick={confirmRollback}
        >
          {targetVersion ? "回退到该版本" : "回退最新版本"}
        </Button>
      </Space>
      {connId && plan && (
        <Typography.Text type="secondary">
          {plannedVersions.length > 0
            ? `将依次回退 ${plannedVersions.length} 个版本:${plannedVersions.join(" → ")}`
            : targetVersion
              ? `已处于版本 ${targetVersion},无需回退`
              : "尚无已成功应用的版本"}
        </Typography.Text>
      )}
    </Space>
  );
}

// ---------------- 页面 ----------------

export default function MigrationsPage() {
  return (
    <Card title="迁移管理" className="ds-tab-page-card">
      <Tabs
        className="ds-fill-tabs"
        defaultActiveKey="libs"
        items={[
          { key: "libs", label: "脚本库管理", children: <LibraryTab /> },
          { key: "run", label: "执行计划", children: <RunTab /> },
          { key: "rollback", label: "回退", children: <RollbackTab /> },
        ]}
      />
    </Card>
  );
}
