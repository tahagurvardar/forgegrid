import { StrictMode, Component } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, NavLink, Routes, Route } from "react-router-dom";
import {
  AttemptPage,
  JobPage,
  JobsPage,
  OverviewPage,
  PipelinePage,
  PipelinesPage,
  SubmitPage,
  WorkersPage,
} from "./pages";
import "./styles.css";
class Boundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  render() {
    return this.state.failed ? (
      <main>
        <h1>Console rendering unavailable</h1>
        <p role="alert">
          Reload to request a fresh snapshot. Backend execution is independent
          of this browser.
        </p>
        <button onClick={() => location.reload()}>Reload console</button>
      </main>
    ) : (
      this.props.children
    );
  }
}
function App() {
  return (
    <BrowserRouter>
      <a className="skip" href="#content">
        Skip to execution view
      </a>
      <aside className="sidebar">
        <NavLink to="/" className="brand">
          <span className="brand-mark" aria-hidden="true">
            ▦
          </span>
          <strong>ForgeGrid</strong>
          <small>EXECUTION CONSOLE</small>
        </NavLink>
        <nav aria-label="Primary navigation">
          {[
            ["/", "System overview"],
            ["/pipelines", "Pipelines"],
            ["/jobs", "Jobs"],
            ["/workers", "Workers"],
          ].map(([path, label]) => (
            <NavLink key={path} to={path ?? "/"} end>
              {label}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-footer">
          <span className="mono">LOCAL / TRUSTED WORKLOADS</span>
          <p>PostgreSQL coordination authority</p>
          <a href="http://localhost:16686" target="_blank" rel="noreferrer">
            Jaeger ↗
          </a>
          <a href="http://localhost:9092" target="_blank" rel="noreferrer">
            Prometheus ↗
          </a>
          <small>
            External telemetry availability is independent of execution.
          </small>
        </div>
      </aside>
      <main id="content">
        <Routes>
          <Route path="/" element={<OverviewPage />} />
          <Route path="/pipelines" element={<PipelinesPage />} />
          <Route path="/pipelines/new" element={<SubmitPage />} />
          <Route path="/pipelines/:id" element={<PipelinePage />} />
          <Route path="/jobs" element={<JobsPage />} />
          <Route path="/jobs/:id" element={<JobPage />} />
          <Route path="/attempts/:id" element={<AttemptPage />} />
          <Route path="/workers" element={<WorkersPage />} />
          <Route
            path="*"
            element={
              <>
                <h1>View not found</h1>
                <NavLink to="/">Return to system overview</NavLink>
              </>
            }
          />
        </Routes>
        <footer className="page-footer">
          Browser-local display time · exact timestamp in tooltip · physical
          execution is at least once
        </footer>
      </main>
    </BrowserRouter>
  );
}
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Boundary>
      <App />
    </Boundary>
  </StrictMode>,
);
