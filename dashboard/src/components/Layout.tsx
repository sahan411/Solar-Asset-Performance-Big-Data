import { NavLink, Outlet } from "react-router-dom";

export function Layout() {
  return (
    <div className="app-shell">
      <header className="app-header">
        <span className="app-title">SolarIQ</span>
        <nav className="app-nav">
          <NavLink to="/" end className={({ isActive }) => (isActive ? "active" : undefined)}>
            Portfolio
          </NavLink>
          <NavLink to="/alerts" className={({ isActive }) => (isActive ? "active" : undefined)}>
            Alerts
          </NavLink>
          <NavLink to="/reports/daily" className={({ isActive }) => (isActive ? "active" : undefined)}>
            Daily Report
          </NavLink>
        </nav>
      </header>
      <main className="app-main">
        <Outlet />
      </main>
    </div>
  );
}
