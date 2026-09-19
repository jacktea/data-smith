import { ExclamationCircleOutlined } from "@ant-design/icons";
import {
  Alert,
  App,
  Button,
  Card,
  Checkbox,
  Select,
  Space,
  Spin,
  Steps,
  Typography,
} from "antd";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api, errMsg } from "../api";
import { useConnections } from "../hooks";

export default function ResetPage() {
  const { message, modal } = App.useApp();
  const navigate = useNavigate();
  const { connections, loading: connsLoading, reload } = useConnections(message.error);
  const [connId, setConnId] = useState<string>();
  const [previewSql, setPreviewSql] = useState<string | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [confirmed, setConfirmed] = useState(false);
  const [executing, setExecuting] = useState(false);

  const connOptions = connections.map((c) => ({
    value: c.id,
    label: `${c.name}(${c.type === "mysql" ? "MySQL" : "PostgreSQL"} · ${c.host}:${c.port}/${c.dbname})`,
  }));

  const step = previewSql == null ? 0 : confirmed ? 2 : 1;

  const loadPreview = async () => {
    if (!connId) return;
    setPreviewing(true);
    setConfirmed(false);
    try {
      const r = await api.resetPreview({ connectionId: connId });
      setPreviewSql(r.sql);
    } catch (e) {
      setPreviewSql(null);
      message.error(errMsg(e));
    } finally {
      setPreviewing(false);
    }
  };

  const execReset = async () => {
    if (!connId) return;
    setExecuting(true);
    try {
      await api.createJobReset({ connectionId: connId, confirmed: true });
      message.success("重置任务已提交,正在跳转任务列表");
      navigate("/jobs");
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setExecuting(false);
    }
  };

  const confirmExec = () => {
    modal.confirm({
      title: "第二次确认:确定要重置该数据库?",
      icon: <ExclamationCircleOutlined />,
      content:
        "重置将清空并重建该库的对象与数据,操作不可逆。请确认这是你选定的 source 库,且预览 SQL 已经过人工审阅。",
      okText: "确认执行重置",
      okButtonProps: { danger: true },
      cancelText: "取消",
      onOk: () => execReset(),
    });
  };

  return (
    <Card title="数据重置(Reset)">
      <Alert
        type="error"
        showIcon
        style={{ marginBottom: 16 }}
        message="高危破坏性操作"
        description="重置会清空并重建目标库的对象与数据;系统库、危险 schema 会被服务端直接拒绝。必须先预览 SQL,再显式确认执行(双重确认)。"
      />
      <Steps
        size="small"
        style={{ marginBottom: 24, maxWidth: 640 }}
        current={step}
        items={[{ title: "选择连接" }, { title: "预览重置 SQL" }, { title: "确认执行" }]}
      />
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        <Space wrap>
          <Typography.Text strong>连接(source,将被重置的库):</Typography.Text>
          <Select
            showSearch
            optionFilterProp="label"
            style={{ width: 340 }}
            placeholder="选择要重置的连接"
            loading={connsLoading}
            options={connOptions}
            value={connId}
            onChange={(v) => {
              setConnId(v);
              setPreviewSql(null);
              setConfirmed(false);
            }}
          />
          <Button type="primary" loading={previewing} disabled={!connId} onClick={() => void loadPreview()}>
            生成预览 SQL(不连接,按配置生成)
          </Button>
          <Button size="small" onClick={() => void reload()} loading={connsLoading}>
            刷新
          </Button>
        </Space>
        {previewing && <Spin tip="正在生成预览..." />}
        {previewSql != null && (
          <>
            <Typography.Text type="danger" strong>
              以下 SQL 将在执行时作用于所选库,请人工逐行审阅:
            </Typography.Text>
            <pre className="ds-danger">{previewSql}</pre>
            <Checkbox checked={confirmed} onChange={(e) => setConfirmed(e.target.checked)}>
              <Typography.Text type="danger" strong>
                我已确认:预览 SQL 审阅无误,接受清空并重建该库
              </Typography.Text>
            </Checkbox>
            <div>
              <Button
                type="primary"
                danger
                size="large"
                disabled={!confirmed}
                loading={executing}
                onClick={confirmExec}
              >
                执行重置
              </Button>
            </div>
          </>
        )}
      </Space>
    </Card>
  );
}
