import type { TaskStatus } from "../types";

const labels: Record<TaskStatus, string> = {
  ready: "Pronta",
  running: "Executando",
  waiting_approval: "Aguardando aprovação",
  succeeded: "Concluída",
  failed: "Falhou",
  cancelled: "Cancelada",
};

export function StatusBadge({ status }: { status: TaskStatus }) {
  return <span className={`status status-${status}`}>{labels[status]}</span>;
}
