import { DownloadOutlined, ReloadOutlined } from "@ant-design/icons";
import {
  Alert,
  App,
  Button,
  Card,
  Descriptions,
  Drawer,
  Popconfirm,
  Progress,
  Space,
  Spin,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import type { DescriptionsProps } from "antd";
import type { ColumnsType } from "antd/es/table";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  api,
  artifactFileUrl,
  artifactZipUrl,
  errMsg,
  type Artifact,
  type ColumnMod,
  type Job,
  type JobStatus,
  type JobType,
  type SummaryDiffData,
  type SummaryDiffSchema,
  type SummaryMigrate,
} from "../api";
import { fmtDuration, fmtSize, fmtTime } from "../format";
import { RegisterVersionModal } from "../components/RegisterVersionModal";

const TYPE_LABEL: Record<JobType, string> = {
  "diff-schema": "结构比对",
  "diff-data": "数据比对",
  "exec-sql": "SQL 脚本",
  reset: "数据重置",
  migrate: "迁移",
  rollback: "回退",
};

const STATUS_META: Record<JobStatus, { label: string; color: string }> = {
  queued: { label: "排队中", color: "default" },
  running: { label: "执行中", color: "processing" },
  succeeded: { label: "成功", color: "success" },
  failed: { label: "失败", color: "error" },
  cancelled: { label: "已取消", color: "default" },
};

function StatusTag({ s }: { s: JobStatus }) {
  const m = STATUS_META[s] ?? { label: s, color: "default" };
  return <Tag color={m.color}>{m.label}</Tag>;
}

function SummaryView({ job }: { job: Job }) {
  const s = job.summary;
  if (!s) return null;
  if (job.type === "diff-schema") {
    const d = s as SummaryDiffSchema;
    // 服务端历史版本可能缺 schema 字段或切片为 null,兜底避免渲染崩溃
    const sc = d.schema ?? { tablesAdded: [], tablesDropped: [], tablesModified: [], destructive: false };
    const tablesModified = sc.tablesModified ?? [];
    return (
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        {sc.destructive && (
          <Alert
            type="error"
            showIcon
            message="该差异包含破坏性变更(删表/删列等),执行前请务必人工审阅 SQL!"
          />
        )}
        <Descriptions
          bordered
          size="small"
          column={3}
          items={[
            { key: "add", label: "新增表", children: sc.tablesAdded?.length ? sc.tablesAdded.join(", ") : "无" },
            { key: "drop", label: "删除表", children: sc.tablesDropped?.length ? sc.tablesDropped.join(", ") : "无" },
            { key: "mod", label: "变更表数", children: String(tablesModified.length) },
          ]}
        />
        {tablesModified.length > 0 && (
          <Table
            rowKey="table"
            size="small"
            pagination={false}
            dataSource={tablesModified}
            columns={[
              { title: "表", dataIndex: "table" },
              {
                title: "新增列",
                dataIndex: "columnsAdded",
                ellipsis: true,
                render: (v: string[]) => (v ?? []).join(", ") || "-",
              },
              {
                title: "删除列",
                dataIndex: "columnsDropped",
                ellipsis: true,
                render: (v: string[]) => ((v ?? []).length ? <Typography.Text type="danger">{(v ?? []).join(", ")}</Typography.Text> : "-"),
              },
              {
                title: "修改列",
                dataIndex: "columnsModified",
                ellipsis: true,
                render: (v: ColumnMod[]) =>
                  (v ?? []).length > 0 ? (
                    <Tooltip title={(v ?? []).map((c) => `${c.name}:${c.detail}`).join("\n")}>
                      <span>{(v ?? []).map((c) => c.name).join(", ")}</span>
                    </Tooltip>
                  ) : (
                    "-"
                  ),
              },
              {
                title: "主键变更",
                dataIndex: "primaryKeyChanged",
                width: 90,
                render: (v: boolean) => (v ? <Tag color="orange">是</Tag> : "否"),
              },
              {
                title: "注释变更",
                dataIndex: "commentChanged",
                width: 90,
                render: (v: boolean) => (v ? "是" : "否"),
              },
            ]}
          />
        )}
      </Space>
    );
  }
  if (job.type === "diff-data") {
    const d = s as SummaryDiffData;
    return (
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        {!d.complete && <Alert type="warning" showIcon message="部分表比对失败,结果不完整,详见下表与日志。" />}
        <Table
          rowKey="table"
          size="small"
          pagination={{ pageSize: 10 }}
          dataSource={d.tables ?? []}
          columns={[
            { title: "表", dataIndex: "table" },
            { title: "新增行", dataIndex: "added", width: 90 },
            { title: "修改行", dataIndex: "modified", width: 90 },
            { title: "删除行", dataIndex: "dropped", width: 90 },
            {
              title: "状态",
              dataIndex: "status",
              width: 90,
              render: (v: string) => (v === "ok" ? <Tag color="success">完成</Tag> : <Tag color="error">失败</Tag>),
            },
            {
              title: "错误",
              dataIndex: "error",
              ellipsis: true,
              render: (v: string) => (v ? <Typography.Text type="danger">{v}</Typography.Text> : "-"),
            },
          ]}
        />
      </Space>
    );
  }
  if (job.type === "exec-sql") {
    const d = s as { mode: string; ok: boolean };
    return (
      <Descriptions
        bordered
        size="small"
        column={2}
        items={[
          { key: "mode", label: "执行模式", children: d.mode },
          {
            key: "ok",
            label: "结果",
            children: d.ok ? <Tag color="success">成功</Tag> : <Tag color="error">失败</Tag>,
          },
        ]}
      />
    );
  }
  if (job.type === "reset") {
    const d = s as { ok: boolean };
    return (
      <Descriptions
        bordered
        size="small"
        column={1}
        items={[
          {
            key: "ok",
            label: "重置结果",
            children: d.ok ? <Tag color="success">成功</Tag> : <Tag color="error">失败</Tag>,
          },
        ]}
      />
    );
  }
  if (job.type === "migrate") {
    const d = s as SummaryMigrate;
    return (
      <Descriptions
        bordered
        size="small"
        column={2}
        items={[
          {
            key: "dry",
            label: "试跑",
            children: d.dryRun ? <Tag color="blue">试跑(已回滚)</Tag> : "正式执行",
          },
          { key: "target", label: "目标版本", children: d.targetVersion || "最新" },
          {
            key: "applied",
            label: "本次应用",
            children: d.applied.length ? d.applied.map((a) => `${a.version} ${a.title}`).join(";") : "无",
          },
        ]}
      />
    );
  }
  const d = s as { version: string; rolled: boolean };
  return (
    <Descriptions
      bordered
      size="small"
      column={1}
      items={[
        {
          key: "rb",
          label: "回退结果",
          children: (
            <>
              版本 <Tag color="orange">{d.version}</Tag>
              {d.rolled ? <Tag color="success">已回退</Tag> : <Tag color="error">未回退</Tag>}
            </>
          ),
        },
      ]}
    />
  );
}

export default function JobsPage() {
  const { message } = App.useApp();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [listLoading, setListLoading] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [detail, setDetail] = useState<Job | null>(null);
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [registerJob, setRegisterJob] = useState<Job | null>(null);
  const detailRef = useRef<Job | null>(null);

  const loadJobs = useCallback(
    async (silent = false) => {
      if (!silent) setListLoading(true);
      try {
        setJobs(await api.listJobs(50));
      } catch (e) {
        if (!silent) message.error(errMsg(e));
      } finally {
        if (!silent) setListLoading(false);
      }
    },
    [message],
  );

  const openDetail = useCallback(
    async (id: string, silent = false) => {
      try {
        const job = await api.getJob(id);
        setDetail(job);
        setDrawerOpen(true);
        try {
          setArtifacts(await api.jobArtifacts(id));
        } catch {
          setArtifacts([]);
        }
      } catch (e) {
        if (!silent) message.error(errMsg(e));
      }
    },
    [message],
  );

  useEffect(() => {
    detailRef.current = detail;
  }, [detail]);

  // 列表自动轮询;详情打开且任务仍在排队/执行时同步刷新详情
  useEffect(() => {
    void loadJobs();
    const t = window.setInterval(() => {
      void loadJobs(true);
      const d = detailRef.current;
      if (d && (d.status === "queued" || d.status === "running")) void openDetail(d.id, true);
    }, 4000);
    return () => window.clearInterval(t);
  }, [loadJobs, openDetail]);

  const cancelJob = async (id: string) => {
    try {
      await api.cancelJob(id);
      message.success("已请求取消");
      await openDetail(id, true);
      void loadJobs(true);
    } catch (e) {
      message.error(errMsg(e));
    }
  };

  const columns: ColumnsType<Job> = [
    {
      title: "任务 ID",
      dataIndex: "id",
      width: 300,
      render: (v: string) => (
        <Typography.Text copyable style={{ fontSize: 12 }}>
          {v}
        </Typography.Text>
      ),
    },
    {
      title: "类型",
      dataIndex: "type",
      width: 110,
      render: (t: JobType) => <Tag>{TYPE_LABEL[t] ?? t}</Tag>,
    },
    {
      title: "状态",
      dataIndex: "status",
      width: 100,
      render: (s: JobStatus) => <StatusTag s={s} />,
    },
    { title: "创建时间", dataIndex: "createdAt", width: 170, render: (v: string) => fmtTime(v) },
    {
      title: "耗时",
      key: "dur",
      width: 110,
      render: (_, j) => fmtDuration(j.startedAt || j.createdAt, j.finishedAt),
    },
    {
      title: "错误",
      dataIndex: "error",
      ellipsis: true,
      render: (v: string) => (v ? <Typography.Text type="danger">{v}</Typography.Text> : "-"),
    },
    {
      title: "操作",
      key: "actions",
      width: 90,
      render: (_, j) => (
        <Button size="small" type="link" onClick={() => void openDetail(j.id)}>
          详情
        </Button>
      ),
    },
  ];

  const descItems: NonNullable<DescriptionsProps["items"]> = [];
  if (detail) {
    descItems.push(
      { key: "id", label: "任务 ID", children: detail.id },
      { key: "status", label: "状态", children: <StatusTag s={detail.status} /> },
      { key: "created", label: "创建时间", children: fmtTime(detail.createdAt) },
      { key: "started", label: "开始时间", children: fmtTime(detail.startedAt) },
      { key: "finished", label: "结束时间", children: fmtTime(detail.finishedAt) },
      {
        key: "dur",
        label: "耗时",
        children: fmtDuration(detail.startedAt || detail.createdAt, detail.finishedAt),
      },
    );
    if (detail.error) {
      descItems.push({
        key: "err",
        label: "错误",
        children: <Typography.Text type="danger">{detail.error}</Typography.Text>,
      });
    }
  }

  return (
    <Card
      title="任务列表(近 50 条,每 4 秒自动刷新)"
      extra={
        <Button icon={<ReloadOutlined />} loading={listLoading} onClick={() => void loadJobs()}>
          刷新
        </Button>
      }
    >
      <Table rowKey="id" size="small" columns={columns} dataSource={jobs} loading={listLoading} pagination={false} />
      <Drawer
        width={760}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        title={detail ? `任务详情 · ${TYPE_LABEL[detail.type] ?? detail.type}` : "任务详情"}
        extra={
          detail && (detail.status === "queued" || detail.status === "running") ? (
            <Popconfirm title="确认取消该任务?" okButtonProps={{ danger: true }} onConfirm={() => void cancelJob(detail.id)}>
              <Button danger size="small">
                取消任务
              </Button>
            </Popconfirm>
          ) : detail?.status === "succeeded" && (detail.type === "diff-schema" || detail.type === "diff-data") ? (
            <Button type="primary" size="small" onClick={() => setRegisterJob(detail)}>
              登记为迁移版本
            </Button>
          ) : undefined
        }
      >
        {!detail ? (
          <Spin />
        ) : (
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            <Descriptions bordered size="small" column={2} items={descItems} />
            {detail.progress && (
              <div>
                <Typography.Title level={5}>进度</Typography.Title>
                <Progress
                  percent={
                    detail.progress.tablesTotal > 0
                      ? Math.round((detail.progress.tablesDone / detail.progress.tablesTotal) * 100)
                      : 0
                  }
                  status={
                    detail.status === "failed"
                      ? "exception"
                      : detail.status === "succeeded"
                        ? "success"
                        : "active"
                  }
                />
                <Typography.Text type="secondary">
                  {detail.progress.tablesDone}/{detail.progress.tablesTotal} 张表
                  {detail.progress.currentTable ? `,当前:${detail.progress.currentTable}` : ""}
                </Typography.Text>
              </div>
            )}
            {detail.summary && (
              <div>
                <Typography.Title level={5}>结果摘要</Typography.Title>
                <SummaryView job={detail} />
              </div>
            )}
            {artifacts.length > 0 && (
              <div>
                <Typography.Title level={5}>产物</Typography.Title>
                <Space style={{ marginBottom: 8 }}>
                  <Button size="small" icon={<DownloadOutlined />} href={artifactZipUrl(detail.id)}>
                    全部下载(zip)
                  </Button>
                </Space>
                <Table
                  rowKey="name"
                  size="small"
                  pagination={false}
                  dataSource={artifacts}
                  columns={[
                    { title: "文件", dataIndex: "name" },
                    { title: "大小", dataIndex: "size", width: 110, render: (v: number) => fmtSize(v) },
                    {
                      title: "下载",
                      key: "dl",
                      width: 80,
                      render: (_, a) => <a href={artifactFileUrl(detail.id, a.name)}>下载</a>,
                    },
                  ]}
                />
              </div>
            )}
            <div>
              <Typography.Title level={5}>参数(敏感字段已由服务端掩码)</Typography.Title>
              <pre className="ds-mono">{JSON.stringify(detail.params, null, 2)}</pre>
            </div>
            <div>
              <Typography.Title level={5}>日志(近 200 行)</Typography.Title>
              <pre className="ds-mono">
                {detail.log.length > 0 ? detail.log.join("\n") : "(暂无日志)"}
              </pre>
            </div>
          </Space>
        )}
      </Drawer>
      {registerJob && (
        <RegisterVersionModal job={registerJob} open onClose={() => setRegisterJob(null)} />
      )}
    </Card>
  );
}
