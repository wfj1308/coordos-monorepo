import { Link } from "react-router-dom";

function ActionItem({ item }) {
  if (item.to) {
    return (
      <Link to={item.to} className={`di-btn ${item.tone || "di-btn-muted"}`}>
        {item.label}
      </Link>
    );
  }

  return (
    <button
      type="button"
      onClick={item.onClick}
      disabled={Boolean(item.disabled)}
      className={`di-btn ${item.tone || "di-btn-muted"} ${item.disabled ? "disabled:cursor-not-allowed disabled:opacity-60" : ""}`}
    >
      {item.label}
    </button>
  );
}

export default function SystemSectionHeader({ kicker, title, subtitle, actions = [] }) {
  return (
    <header className="panel di-console-shell di-module-header p-5">
      <div className="di-console-hero">
        <div>
          <p className="di-kicker">{kicker}</p>
          <h2 className="di-console-title di-module-title">{title}</h2>
          {subtitle ? <p className="di-console-subtitle">{subtitle}</p> : null}
        </div>
        {actions.length > 0 ? (
          <div className="di-console-actions">
            {actions.map((item) => (
              <ActionItem key={`${item.label}-${item.to || "action"}`} item={item} />
            ))}
          </div>
        ) : null}
      </div>
    </header>
  );
}
