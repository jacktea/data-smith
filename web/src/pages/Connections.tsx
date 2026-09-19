import { ApiOutlined, PlusOutlined, ReloadOutlined } from "@ant-design/icons";
import {
  Alert,
  App,
  Button,
  Card,
  Col,
  Collapse,
  Drawer,
  Form,
  Input,
  InputNumber,
  Popconfirm,
  Row,
  Select,
  Space,
  Switch,
  Table,
  Tag,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { useState } from "react";
import {
  api,
  errMsg,
  type Connection,
  type ConnectionInput,
  type DbType,
  type ProxyInput,
  type TestResult,
} from "../api";
import { useConnections } from "../hooks";

interface ConnFormValues {
  name: string;
  type: DbType;
  host: string;
  port: number;
  user: string;
  password?: string;
  dbname: string;
  tableSchema?: string;
  ssl?: boolean;
  connectTimeoutMs?: number;
  maxOpenConns?: number;
  proxyEnabled?: boolean;
  proxyType?: string;
  proxyHost?: string;
  proxyPort?: number;
  proxyUser?: string;
  proxyPassword?: string;
  proxyKnownHostsPath?: string;
  proxyHostFingerprint?: string;
  proxyRsaKeyPath?: string;
}

export default function ConnectionsPage() {
  const { message } = App.useApp();
  const { connections, loading, reload } = useConnections(message.error);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editing, setEditing] = useState<Connection | null>(null);
  const [saving, setSaving] = useState(false);
  const [testingId, setTestingId] = useState<string | null>(null);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [form] = Form.useForm<ConnFormValues>();

  const openCreate = () => {
    setEditing(null);
    setTestResult(null);
    form.resetFields();
    form.setFieldsValue({
      type: "mysql",
      port: 3306,
      ssl: false,
      connectTimeoutMs: 5000,
      maxOpenConns: 10,
      proxyEnabled: false,
      proxyType: "ssh",
    });
    setDrawerOpen(true);
  };

  const openEdit = (c: Connection) => {
    setEditing(c);
    setTestResult(null);
    form.resetFields();
    form.setFieldsValue({
      name: c.name,
      type: c.type,
      host: c.host,
      port: c.port,
      user: c.user,
      dbname: c.dbname,
      tableSchema: c.tableSchema,
      ssl: c.ssl,
      connectTimeoutMs: c.connectTimeoutMs,
      maxOpenConns: c.maxOpenConns,
      proxyEnabled: c.proxy != null,
      proxyType: c.proxy?.type || "ssh",
      proxyHost: c.proxy?.host,
      proxyPort: c.proxy?.port,
      proxyUser: c.proxy?.user,
      proxyKnownHostsPath: c.proxy?.knownHostsPath,
      proxyHostFingerprint: c.proxy?.hostFingerprint,
    });
    setDrawerOpen(true);
  };

  const runTest = async (id: string) => {
    setTestingId(id);
    setTestResult(null);
    try {
      const r = await api.testConnection(id);
      setTestResult(r);
      if (r.ok) {
        message.success(r.serverVersion ? `连接成功 · 服务器版本:${r.serverVersion}` : "连接成功");
      } else {
        message.error(`连接失败:${r.error || "未知错误"}`);
      }
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setTestingId(null);
    }
  };

  const remove = async (c: Connection) => {
    try {
      await api.deleteConnection(c.id);
      message.success("连接已删除");
      void reload();
    } catch (e) {
      message.error(errMsg(e));
    }
  };

  const handleTypeChange = (t: DbType) => {
    const cur = form.getFieldValue("port") as number | undefined;
    if (!editing) {
      form.setFieldValue("port", t === "mysql" ? 3306 : 5432);
    } else if (cur === 3306 && t === "postgres") {
      form.setFieldValue("port", 5432);
    } else if (cur === 5432 && t === "mysql") {
      form.setFieldValue("port", 3306);
    }
  };

  const submit = async () => {
    const v = (await form.validateFields()) as ConnFormValues;
    const proxy: ProxyInput | null = v.proxyEnabled
      ? {
          type: v.proxyType || "ssh",
          host: v.proxyHost ?? "",
          port: v.proxyPort ?? 22,
          user: v.proxyUser ?? "",
          ...(v.proxyPassword ? { password: v.proxyPassword } : {}),
          knownHostsPath: v.proxyKnownHostsPath ?? "",
          hostFingerprint: v.proxyHostFingerprint ?? "",
          ...(v.proxyRsaKeyPath ? { rsaKeyPath: v.proxyRsaKeyPath } : {}),
        }
      : null;
    const body: ConnectionInput = {
      name: v.name,
      type: v.type,
      host: v.host,
      port: v.port,
      user: v.user,
      // 编辑时密码留空 = 保持原密码(缺省字段)
      ...(v.password ? { password: v.password } : {}),
      dbname: v.dbname,
      tableSchema: v.tableSchema ?? "",
      ssl: !!v.ssl,
      proxy,
      connectTimeoutMs: v.connectTimeoutMs ?? 5000,
      maxOpenConns: v.maxOpenConns ?? 10,
    };
    setSaving(true);
    try {
      if (editing) {
        await api.updateConnection(editing.id, body);
        message.success("连接已更新");
      } else {
        await api.createConnection(body);
        message.success("连接已创建");
      }
      setDrawerOpen(false);
      void reload();
    } catch (e) {
      message.error(errMsg(e));
    } finally {
      setSaving(false);
    }
  };

  const columns: ColumnsType<Connection> = [
    { title: "名称", dataIndex: "name", render: (v: string) => <strong>{v}</strong> },
    {
      title: "类型",
      dataIndex: "type",
      width: 110,
      render: (t: DbType) => (
        <Tag color={t === "mysql" ? "blue" : "cyan"}>{t === "mysql" ? "MySQL" : "PostgreSQL"}</Tag>
      ),
    },
    { title: "地址", key: "addr", render: (_, c) => `${c.host}:${c.port}` },
    { title: "用户", dataIndex: "user", width: 120 },
    { title: "数据库", dataIndex: "dbname", width: 130 },
    {
      title: "Schema",
      dataIndex: "tableSchema",
      width: 110,
      render: (v: string) => v || "-",
    },
    {
      title: "SSL",
      dataIndex: "ssl",
      width: 70,
      render: (v: boolean) => (v ? <Tag color="green">是</Tag> : <Tag>否</Tag>),
    },
    {
      title: "SSH 代理",
      key: "proxy",
      width: 150,
      render: (_, c) =>
        c.proxy ? (
          <Tag color="purple">
            {c.proxy.type || "ssh"} · {c.proxy.host}
          </Tag>
        ) : (
          "-"
        ),
    },
    {
      title: "密码",
      dataIndex: "passwordSet",
      width: 90,
      render: (v: boolean) => (v ? <Tag>已配置</Tag> : <Tag color="orange">未配置</Tag>),
    },
    {
      title: "操作",
      key: "actions",
      width: 230,
      render: (_, c) => (
        <Space size="small">
          <Button
            size="small"
            icon={<ApiOutlined />}
            loading={testingId === c.id}
            onClick={() => void runTest(c.id)}
          >
            测试
          </Button>
          <Button size="small" onClick={() => openEdit(c)}>
            编辑
          </Button>
          <Popconfirm
            title="确认删除该连接?"
            description="删除后不可恢复。"
            okButtonProps={{ danger: true }}
            onConfirm={() => void remove(c)}
          >
            <Button size="small" danger>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Card
      title="连接管理"
      extra={
        <Space>
          <Button icon={<ReloadOutlined />} loading={loading} onClick={() => void reload()}>
            刷新
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
            新建连接
          </Button>
        </Space>
      }
    >
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message="密码、代理私钥等敏感字段由服务端掩码保存;编辑时对应输入框留空表示保持原值。"
      />
      <Table rowKey="id" columns={columns} dataSource={connections} loading={loading} pagination={false} />
      <Drawer
        width={560}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        title={editing ? `编辑连接:${editing.name}` : "新建连接"}
        footer={
          <Space style={{ float: "right" }}>
            {editing && (
              <Button
                icon={<ApiOutlined />}
                loading={testingId === editing.id}
                onClick={() => void runTest(editing.id)}
              >
                测试连接
              </Button>
            )}
            <Button onClick={() => setDrawerOpen(false)}>取消</Button>
            <Button type="primary" loading={saving} onClick={() => void submit()}>
              保存
            </Button>
          </Space>
        }
      >
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={12}>
              <Form.Item name="name" label="名称" rules={[{ required: true, message: "请输入连接名称" }]}>
                <Input placeholder="如:生产 MySQL" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="type" label="数据库类型" rules={[{ required: true, message: "请选择类型" }]}>
                <Select
                  options={[
                    { value: "mysql", label: "MySQL" },
                    { value: "postgres", label: "PostgreSQL" },
                  ]}
                  onChange={(t) => handleTypeChange(t as DbType)}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="host" label="主机" rules={[{ required: true, message: "请输入主机" }]}>
                <Input placeholder="127.0.0.1" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="port" label="端口" rules={[{ required: true, message: "请输入端口" }]}>
                <InputNumber style={{ width: "100%" }} min={1} max={65535} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="user" label="用户名" rules={[{ required: true, message: "请输入用户名" }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                name="password"
                label="密码"
                extra={
                  editing
                    ? editing.passwordSet
                      ? "已配置密码;留空表示保持原密码"
                      : "尚未配置密码"
                    : undefined
                }
              >
                <Input.Password
                  autoComplete="new-password"
                  placeholder={editing && editing.passwordSet ? "留空保持原密码" : "请输入密码"}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="dbname" label="数据库名" rules={[{ required: true, message: "请输入数据库名" }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="tableSchema" label="Schema(表空间)" extra="PostgreSQL 的 schema,留空默认 public;MySQL 可留空">
                <Input />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="ssl" label="SSL" valuePropName="checked">
                <Switch checkedChildren="开" unCheckedChildren="关" />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="connectTimeoutMs" label="连接超时(ms)">
                <InputNumber style={{ width: "100%" }} min={1} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="maxOpenConns" label="最大连接数">
                <InputNumber style={{ width: "100%" }} min={1} />
              </Form.Item>
            </Col>
          </Row>
          <Collapse
            items={[
              {
                key: "proxy",
                label: "SSH 代理 / 跳板机(可选)",
                children: (
                  <>
                    <Form.Item name="proxyEnabled" label="启用代理" valuePropName="checked">
                      <Switch checkedChildren="开" unCheckedChildren="关" />
                    </Form.Item>
                    <Form.Item noStyle shouldUpdate={(p, c) => p.proxyEnabled !== c.proxyEnabled}>
                      {({ getFieldValue }) =>
                        getFieldValue("proxyEnabled") ? (
                          <Row gutter={12}>
                            <Col span={12}>
                              <Form.Item name="proxyType" label="代理类型">
                                <Select options={[{ value: "ssh", label: "SSH 隧道" }]} />
                              </Form.Item>
                            </Col>
                            <Col span={12}>
                              <Form.Item
                                name="proxyHost"
                                label="代理主机"
                                rules={[{ required: true, message: "请输入代理主机" }]}
                              >
                                <Input />
                              </Form.Item>
                            </Col>
                            <Col span={12}>
                              <Form.Item
                                name="proxyPort"
                                label="代理端口"
                                rules={[{ required: true, message: "请输入代理端口" }]}
                              >
                                <InputNumber style={{ width: "100%" }} min={1} max={65535} />
                              </Form.Item>
                            </Col>
                            <Col span={12}>
                              <Form.Item
                                name="proxyUser"
                                label="代理用户"
                                rules={[{ required: true, message: "请输入代理用户" }]}
                              >
                                <Input />
                              </Form.Item>
                            </Col>
                            <Col span={12}>
                              <Form.Item
                                name="proxyPassword"
                                label="代理密码"
                                extra={editing?.proxy?.passSet ? "已配置;留空保持原值" : "密码与私钥至少配置一项"}
                              >
                                <Input.Password autoComplete="new-password" />
                              </Form.Item>
                            </Col>
                            <Col span={12}>
                              <Form.Item
                                name="proxyRsaKeyPath"
                                label="私钥路径"
                                extra={editing?.proxy?.rsaKeyPathSet ? "已配置;留空保持原值" : "服务端路径,可选"}
                              >
                                <Input />
                              </Form.Item>
                            </Col>
                            <Col span={12}>
                              <Form.Item name="proxyKnownHostsPath" label="known_hosts 路径">
                                <Input />
                              </Form.Item>
                            </Col>
                            <Col span={12}>
                              <Form.Item name="proxyHostFingerprint" label="主机指纹">
                                <Input />
                              </Form.Item>
                            </Col>
                          </Row>
                        ) : (
                          <Typography.Text type="secondary">未启用代理</Typography.Text>
                        )
                      }
                    </Form.Item>
                  </>
                ),
              },
            ]}
          />
          {testResult && (
            <Alert
              style={{ marginTop: 16 }}
              type={testResult.ok ? "success" : "error"}
              showIcon
              message={
                testResult.ok
                  ? `连接成功${testResult.serverVersion ? ` · 服务器版本:${testResult.serverVersion}` : ""}`
                  : `连接失败:${testResult.error || "未知错误"}`
              }
            />
          )}
        </Form>
      </Drawer>
    </Card>
  );
}
