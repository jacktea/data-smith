import {
  CodeOutlined,
  DatabaseOutlined,
  DiffOutlined,
  PartitionOutlined,
  RocketOutlined,
  TableOutlined,
  UnorderedListOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import { Layout, Menu, Typography } from "antd";
import { Navigate, Outlet, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import ConnectionsPage from "./pages/Connections";
import DiffDataPage from "./pages/DiffData";
import DiffFullPage from "./pages/DiffFull";
import DiffSchemaPage from "./pages/DiffSchema";
import JobsPage from "./pages/Jobs";
import MigrationsPage from "./pages/Migrations";
import ResetPage from "./pages/ResetPage";
import SqlConsolePage from "./pages/SqlConsole";

const { Sider, Header, Content } = Layout;

const MENU_ITEMS = [
  { key: "/connections", icon: <DatabaseOutlined />, label: "连接管理" },
  { key: "/diff-schema", icon: <PartitionOutlined />, label: "结构比对" },
  { key: "/diff-data", icon: <TableOutlined />, label: "数据比对" },
  { key: "/diff-full", icon: <DiffOutlined />, label: "完全比对" },
  { key: "/sql", icon: <CodeOutlined />, label: "SQL 控制台" },
  { key: "/reset", icon: <WarningOutlined />, label: "数据重置" },
  { key: "/migrations", icon: <RocketOutlined />, label: "迁移管理" },
  { key: "/jobs", icon: <UnorderedListOutlined />, label: "任务列表" },
];

const TAB_SCROLL_ROUTES = ["/migrations", "/sql"];

function MainLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const isTabScroll = TAB_SCROLL_ROUTES.some((r) => location.pathname.startsWith(r));

  return (
    <Layout style={{ height: "100vh", overflow: "hidden" }}>
      <Sider
        width={208}
        theme="dark"
        style={{
          height: "100vh",
          display: "flex",
          flexDirection: "column",
          flexShrink: 0,
          overflow: "hidden",
        }}
      >
        <div
          style={{
            height: 56,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: "#fff",
            fontWeight: 700,
            fontSize: 16,
            letterSpacing: 1,
            flexShrink: 0,
            borderBottom: "1px solid rgba(255, 255, 255, 0.08)",
          }}
        >
          DataSmith 控制台
        </div>
        <div style={{ flex: 1, minHeight: 0, overflowY: "auto", overflowX: "hidden" }}>
          <Menu
            mode="inline"
            theme="dark"
            selectedKeys={[location.pathname]}
            items={MENU_ITEMS}
            onClick={({ key }) => navigate(key)}
          />
        </div>
      </Sider>
      <Layout
        style={{
          height: "100vh",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          flex: 1,
          minWidth: 0,
        }}
      >
        <Header
          style={{
            background: "#fff",
            padding: "0 24px",
            height: 56,
            lineHeight: "56px",
            borderBottom: "1px solid #f0f0f0",
            flexShrink: 0,
          }}
        >
          <Typography.Text type="secondary">
            数据库结构/数据比对、迁移与重置控制台 · 术语约定:source=将被变更的库,target=参照标准
          </Typography.Text>
        </Header>
        <Content className={isTabScroll ? "ds-content-tab-mode" : "ds-content-main-mode"}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}

export default function App() {
  return (
    <Routes>
      <Route element={<MainLayout />}>
        <Route index element={<Navigate to="/connections" replace />} />
        <Route path="/connections" element={<ConnectionsPage />} />
        <Route path="/diff-schema" element={<DiffSchemaPage />} />
        <Route path="/diff-data" element={<DiffDataPage />} />
        <Route path="/diff-full" element={<DiffFullPage />} />
        <Route path="/sql" element={<SqlConsolePage />} />
        <Route path="/reset" element={<ResetPage />} />
        <Route path="/migrations" element={<MigrationsPage />} />
        <Route path="/jobs" element={<JobsPage />} />
        <Route path="*" element={<Navigate to="/connections" replace />} />
      </Route>
    </Routes>
  );
}
