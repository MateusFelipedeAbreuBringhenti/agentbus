import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import { TaskDetail } from "./pages/TaskDetail";
import { TaskList } from "./pages/TaskList";
import { TraceView } from "./pages/TraceView";

const deferred = ["Agentes AI", "Aprovações", "Log de Auditoria", "Configurações"];

function Shell() {
  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand"><span className="brand-mark">ϟ</span><div><strong>AgentBus</strong><small>CONTROL CENTER</small></div></div>
        <p className="nav-heading">OPERAÇÃO LOCAL</p>
        <nav aria-label="Navegação principal">
          <NavLink to="/tasks">▣ <span>Tarefas</span></NavLink>
          <NavLink to="/trace">⌁ <span>Rastreamento Causal</span></NavLink>
          {deferred.map((label) => <span className="nav-disabled" key={label}>◇ <span>{label}</span><em>posterior</em></span>)}
        </nav>
        <div className="local-badge"><i /> SOMENTE LOCAL<small>Leitura real · sem mutações</small></div>
      </aside>
      <div className="workspace">
        <header><span>AGENTBUS / <b>Control Center</b></span><div className="connection"><i /> LOOPBACK</div></header>
        <div className="truth-banner">◈ DADOS REAIS DO AGENTBUS · INTERFACE SOMENTE LEITURA</div>
        <main><Routes>
          <Route path="/tasks" element={<TaskList />} />
          <Route path="/tasks/:taskId" element={<TaskDetail />} />
          <Route path="/correlations/:correlationId" element={<TraceView />} />
          <Route path="/trace" element={<TraceView />} />
          <Route path="*" element={<Navigate to="/tasks" replace />} />
        </Routes></main>
      </div>
    </div>
  );
}

export function App() { return <Shell />; }
