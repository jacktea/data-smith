import { useEffect, useState } from "react";
import { Alert, App, Checkbox, Input, Modal, Select, Space, Tag, Typography } from "antd";
import { api, errMsg, type Job, type Library } from "../api";
import { useConnections } from "../hooks";

/** 任务详情抽屉内「登记为迁移版本」弹窗(契约第 4 节) */
export function RegisterVersionModal({
  job,
  open,
  onClose,
}: {
  job: Job;
  open: boolean;
  onClose: () => void;
}) {
  const { message } = App.useApp();
  const { connections } = useConnections(message.error);
  const [libs, setLibs] = useState<Library[]>([]);
  const [libId, setLibId] = useState<string>();
  const [expectedConnId, setExpectedConnId] = useState<string>();
  const [companionJobs, setCompanionJobs] = useState<Job[]>([]);
  const [companionJobId, setCompanionJobId] = useState<string>();
  // 完全对比任务单一目录内同时含结构与数据产物,无需配套任务
  const isFull = job.type === "diff-full";
  const companionType = job.type === "diff-schema" ? "diff-data" : "diff-schema";
  const [includeSchema, setIncludeSchema] = useState(job.type !== "diff-data");
  const [includeData, setIncludeData] = useState(job.type !== "diff-schema");
  const [version, setVersion] = useState("");
  const [title, setTitle] = useState("");
  const [nextVersion, setNextVersion] = useState("");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) return;
    setExpectedConnId(
      typeof job.params.sourceId === "string" && job.params.sourceId ? job.params.sourceId : undefined,
    );
    setIncludeSchema(job.type !== "diff-data");
    setIncludeData(job.type !== "diff-schema");
    setVersion("");
    setNextVersion("");
    api
      .listLibraries()
      .then(setLibs)
      .catch((e) => message.error(errMsg(e)));
    if (isFull) {
      setCompanionJobs([]);
      setCompanionJobId(undefined);
      return;
    }
    api
      .listJobs(100)
      .then((jobs) =>
        setCompanionJobs(jobs.filter((j) => j.type === companionType && j.status === "succeeded")),
      )
      .catch(() => setCompanionJobs([]));
  }, [open, job, message, companionType, isFull]);

  // 选择配套任务后自动纳入其变更段
  useEffect(() => {
    if (companionJobId) {
      if (companionType === "diff-data") setIncludeData(true);
      else setIncludeSchema(true);
    }
  }, [companionJobId, companionType]);

  // 选定脚本库+预期执行库后拉取计划,预填 nextVersion
  useEffect(() => {
    if (!open || !libId || !expectedConnId) return;
    let stale = false;
    api
      .migratePlan(libId, expectedConnId)
      .then((p) => {
        if (stale) return;
        setNextVersion(p.nextVersion);
        setVersion((v) => v || p.nextVersion);
      })
      .catch(() => {
        if (!stale) setNextVersion("");
      });
    return () => {
      stale = true;
    };
  }, [open, libId, expectedConnId]);

  const canSubmit =
    !!libId && !!expectedConnId && version.trim() !== "" && title.trim() !== "" && (includeSchema || includeData);

  const submit = async () => {
    if (!libId || !expectedConnId) {
      message.warning("请选择目标脚本库与预期执行库");
      return;
    }
    if (!version.trim() || !title.trim()) {
      message.warning("请填写版本号与标题(将用于生成 V{版本}__{标题}.up.sql 文件名)");
      return;
    }
    if (!includeSchema && !includeData) {
      message.warning("请至少勾选一项:结构变更 / 数据变更");
      return;
    }
    setSubmitting(true);
    try {
      const r = await api.registerVersion(libId, {
        sourceJobId: job.id,
        companionJobId: companionJobId || undefined,
        includeSchema,
        includeData,
        version: version.trim(),
        title: title.trim(),
        expectedConnectionId: expectedConnId,
      });
      message.success(`已登记版本:up=${r.upFile},down=${r.downFile}`);
      onClose();
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      title="登记为迁移版本"
      open={open}
      onCancel={onClose}
      onOk={() => void submit()}
      okButtonProps={{ disabled: !canSubmit }}
      confirmLoading={submitting}
      okText="登记"
      cancelText="取消"
      width={560}
    >
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        <Alert
          type="info"
          showIcon
          message="将把该任务的产物登记为脚本库新版本"
          description={
            <>
              up = 正向 SQL 串接(结构 → 数据);down = 回滚 SQL 串接(数据 → 结构)。文件名形如{" "}
              <Tag>
                V{version || nextVersion || "版本"}__{title || "标题"}.up.sql
              </Tag>
            </>
          }
        />
        <div>
          <Typography.Text strong>目标脚本库</Typography.Text>
          <Select
            style={{ width: "100%", marginTop: 4 }}
            placeholder="选择脚本库"
            value={libId}
            onChange={(v) => setLibId(v)}
            options={libs.map((l) => ({ value: l.id, label: l.name }))}
          />
        </div>
        <div>
          <Typography.Text strong>预期执行库(默认取任务 source)</Typography.Text>
          <Select
            style={{ width: "100%", marginTop: 4 }}
            placeholder="该版本预期执行的连接"
            value={expectedConnId}
            onChange={(v) => setExpectedConnId(v)}
            options={connections.map((c) => ({
              value: c.id,
              label: `${c.name}(${c.type === "mysql" ? "MySQL" : "PostgreSQL"} · ${c.host}:${c.port}/${c.dbname})`,
            }))}
          />
          <Typography.Text type="secondary">
            与比对 source 不一致时警告放行;数据库方言不匹配将硬拒绝。
          </Typography.Text>
        </div>
        <Space size="large">
          <Checkbox
            checked={includeSchema}
            disabled={isFull || (job.type !== "diff-schema" && !companionJobId)}
            onChange={(e) => setIncludeSchema(e.target.checked)}
          >
            包含结构变更(schema)
          </Checkbox>
          <Checkbox
            checked={includeData}
            disabled={isFull || (job.type !== "diff-data" && !companionJobId)}
            onChange={(e) => setIncludeData(e.target.checked)}
          >
            包含数据变更(data)
          </Checkbox>
        </Space>
        {!isFull && (
          <div>
            <Typography.Text strong>
              关联{companionType === "diff-data" ? "数据" : "结构"}比对任务(可选,并入同一版本)
            </Typography.Text>
            <Select
              style={{ width: "100%", marginTop: 4 }}
              allowClear
              placeholder={`选择已成功的 ${companionType === "diff-data" ? "数据" : "结构"}比对任务,合并登记为一个版本`}
              value={companionJobId}
              onChange={(v) => setCompanionJobId(v || undefined)}
              options={companionJobs.map((j) => ({
                value: j.id,
                label: `${j.id} · ${j.createdAt ?? ""}`,
              }))}
            />
          </div>
        )}
        <Space size="middle" style={{ width: "100%" }}>
          <div style={{ flex: 1 }}>
            <Typography.Text strong>版本号</Typography.Text>
            <Input
              style={{ marginTop: 4 }}
              value={version}
              onChange={(e) => setVersion(e.target.value)}
              placeholder={nextVersion ? `下一版本:${nextVersion}` : "如 1.0.1"}
            />
          </div>
          <div style={{ flex: 1 }}>
            <Typography.Text strong>标题</Typography.Text>
            <Input
              style={{ marginTop: 4 }}
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder="将进入文件名,如 sync-user-table"
            />
          </div>
        </Space>
      </Space>
    </Modal>
  );
}
