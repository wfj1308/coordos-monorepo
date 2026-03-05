import { BrowserRouter, Link, NavLink, Navigate, Outlet, Route, Routes, useLocation } from "react-router-dom";
import DashboardPage from "./DashboardPage";
import MainFlowPage from "./MainFlowPage";
import ApiConsolePage from "./ApiConsolePage";
import PartnerProfilePage from "./PartnerProfilePage";
import JoinPage from "./JoinPage";

const portalModules = [
  {
    key: "dashboard",
    title: "业务数据看板",
    desc: "项目、合同、人员、资质、业绩与三库映射统一回读。",
    href: "/dashboard",
    badge: "核心",
    icon: "KANBAN",
  },
  {
    key: "partner-profile",
    title: "合作能力画像",
    desc: "对外展示资质层、能力层、业绩层、当前产能与验真能力。",
    href: "/partner-profile",
    badge: "公开接口",
    icon: "PROFILE",
  },
  {
    key: "main-flow",
    title: "Phase0-7 主流程",
    desc: "覆盖注册入网到业绩入池的全链路联调与状态追踪。",
    href: "/main-flow",
    badge: "流程联调",
    icon: "FLOW",
  },
  {
    key: "api-console",
    title: "API 联调控制台",
    desc: "统一配置 DI/Vault 调试环境，快速回放模板请求。",
    href: "/api-console",
    badge: "开发工具",
    icon: "API",
  },
  {
    key: "join",
    title: "设计院入网注册",
    desc: "四步完成组织入网、资质注册、执行体导入与激活上线。",
    href: "/join",
    badge: "入网",
    icon: "JOIN",
  },
];

const quickChecks = ["统一入口与权限边界", "主流程联调闭环可追踪", "三库数据回读与质量检查", "对外能力声明与验真"];

function SystemHome() {
  return (
    <section className="di-system-home space-y-4">
      <header className="panel di-console-shell p-5">
        <div className="di-console-hero">
          <div>
            <p className="di-kicker">CoordOS / Design Institute</p>
            <h1 className="di-console-title">中北设计院管理系统</h1>
            <p className="di-console-subtitle">将业务联调、数据看板、入网注册、能力声明和 API 工具纳入同一工作台。</p>
            <div className="di-console-meta">
              <span>系统化导航</span>
              <span>统一视觉语言</span>
              <span>模块独立可扩展</span>
            </div>
          </div>
          <div className="di-console-actions">
            <Link to="/dashboard" className="di-btn di-btn-primary">
              进入业务看板
            </Link>
            <Link to="/join" className="di-btn di-btn-muted">
              打开入网流程
            </Link>
          </div>
        </div>
      </header>

      <section className="di-system-kpi-grid">
        {quickChecks.map((item) => (
          <article key={item} className="di-system-kpi-card">
            <p>{item}</p>
          </article>
        ))}
      </section>

      <section className="di-portal-grid">
        {portalModules.map((module) => (
          <Link key={module.key} to={module.href} className="di-portal-card">
            <div className="di-portal-card-head">
              <div className="di-portal-badge">{module.badge}</div>
              <div className="di-portal-icon">{module.icon}</div>
            </div>
            <h2 className="di-portal-title">{module.title}</h2>
            <p className="di-portal-desc">{module.desc}</p>
            <span className="di-portal-link">打开模块</span>
          </Link>
        ))}
      </section>
    </section>
  );
}

function ShellLayout() {
  const location = useLocation();
  const activeModule = portalModules.find((item) => location.pathname.startsWith(item.href));
  const activeTitle = activeModule ? activeModule.title : "系统总览";

  return (
    <div className="di-app-shell">
      <aside className="di-side-nav">
        <div className="di-side-brand">
          <p className="di-side-kicker">COORDOS</p>
          <h2>设计院管理系统</h2>
        </div>
        <nav aria-label="主导航" className="di-nav-list">
          <NavLink to="/" end className={({ isActive }) => `di-nav-item ${isActive ? "is-active" : ""}`}>
            首页总览
          </NavLink>
          {portalModules.map((module) => (
            <NavLink
              key={module.key}
              to={module.href}
              className={({ isActive }) => `di-nav-item ${isActive ? "is-active" : ""}`}
            >
              {module.title}
            </NavLink>
          ))}
        </nav>
      </aside>

      <section className="di-app-main">
        <header className="di-app-topbar">
          <div>
            <p className="di-app-breadcrumb">系统工作区 / {activeTitle}</p>
            <h1>{activeTitle}</h1>
          </div>
          <div className="di-app-health">
            <span>环境: {localStorage.getItem("coordos.di.base") || "/di"}</span>
            <span>状态: ONLINE</span>
          </div>
        </header>
        <main className="di-app-content">
          <Outlet />
        </main>
      </section>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<ShellLayout />}>
          <Route path="/" element={<SystemHome />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/main-flow" element={<MainFlowPage />} />
          <Route path="/api-console" element={<ApiConsolePage />} />
          <Route path="/partner-profile" element={<PartnerProfilePage />} />
          <Route path="/join/*" element={<JoinPage />} />
        </Route>
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}
