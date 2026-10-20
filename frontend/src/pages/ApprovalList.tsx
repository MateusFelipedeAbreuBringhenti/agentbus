import { FormEvent, useCallback, useState } from "react";
import { createApprovalIntent, getTask, listApprovals } from "../api";
import { EmptyState, ErrorState, Loading } from "../components/AsyncState";
import { useLoad } from "../hooks/useLoad";
import type { Approval, ApprovalIntent, ApprovalStatus } from "../types";

function ReleaseState({ approval }: { approval: Approval }) {
  const load = useCallback((signal: AbortSignal) => getTask(approval.task_id, signal), [approval.task_id]);
  const { data: task, error, loading } = useLoad(load, [load]);
  if (loading) return <p className="release-state">Consultando estado factual da Task…</p>;
  if (error || !task) return <p className="release-state unavailable">Estado de release indisponível.</p>;
  if (task.status === "waiting_approval" && task.waiting_on_approval_id === approval.id) {
    return <p className="release-state waiting">Approval aprovada; Task ainda aguarda release.</p>;
  }
  if (task.status === "ready" && task.waiting_on_approval_id === null) {
    return <p className="release-state released">Task liberada e pronta para execução.</p>;
  }
  return <p className="release-state">Estado atual da Task: <b>{task.status}</b>.</p>;
}

export function ApprovalList() {
  const [assignee, setAssignee] = useState("");
  const [filter, setFilter] = useState("");
  const [status, setStatus] = useState<ApprovalStatus>("pending");
  const [selected, setSelected] = useState<Approval | null>(null);
  const [decision, setDecision] = useState<"approve" | "reject">("approve");
  const [reason, setReason] = useState("");
  const [intent, setIntent] = useState<ApprovalIntent | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const load = useCallback((signal: AbortSignal) => listApprovals({ assignedTo: filter || undefined, status }, signal), [filter, status]);
  const { data, error, loading, retry } = useLoad(load, [load]);
  function apply(event: FormEvent) { event.preventDefault(); setFilter(assignee); }
  function prepare(approval: Approval, next: "approve" | "reject") { setSelected(approval); setDecision(next); setReason(""); setIntent(null); setActionError(null); }
  async function register() {
    if (!selected) return;
    setSubmitting(true); setActionError(null);
    try { setIntent(await createApprovalIntent(selected, decision, reason || null)); }
    catch (caught) { setActionError(caught instanceof Error ? caught.message : "Falha desconhecida."); }
    finally { setSubmitting(false); }
  }
  return <>
    <section className="page-heading"><div><small>GOVERNANÇA · DECISÃO HUMANA</small><h1>Aprovações</h1><p>O painel registra intenções e mostra fatos; somente processos autenticados decidem ou liberam.</p></div><span className="readonly-pill">SEM CREDENCIAL HUMANA</span></section>
    <div className="warning-banner"><strong>Decisão ≠ reação</strong> Aprovar ou rejeitar não libera nem executa automaticamente a Task.</div>
    <form className="filters" onSubmit={apply} aria-label="Filtros de aprovações"><input aria-label="Responsável" placeholder="Responsável" value={assignee} onChange={(event) => setAssignee(event.target.value)} /><select aria-label="Status" value={status} onChange={(event) => setStatus(event.target.value as ApprovalStatus)}><option value="pending">Pendentes</option><option value="approved">Aprovadas</option><option value="rejected">Rejeitadas</option></select><button type="submit">Aplicar filtro</button></form>
    {loading && <Loading />}{error && <ErrorState message={error} retry={retry} />}
    {!loading && !error && data?.items.length === 0 && <EmptyState title="Nenhuma Approval encontrada" message="O AgentBus respondeu normalmente para estes filtros." />}
    {!loading && !error && data && data.items.length > 0 && <section className="approval-grid">{data.items.map((approval) => <article className="approval-card" key={approval.id}><small>{approval.gate} · {approval.status} · v{approval.version}</small><h2>{approval.request_reason}</h2><p>Responsável: <b>{approval.assigned_to}</b></p><p>Task: <span className="mono">{approval.task_id}</span></p>{approval.status === "approved" && <ReleaseState approval={approval} />}{approval.status === "pending" && <div className="approval-actions"><button onClick={() => prepare(approval, "approve")}>Solicitar aprovação</button><button className="danger" onClick={() => prepare(approval, "reject")}>Solicitar rejeição</button></div>}</article>)}</section>}
    {selected && !intent && <section className="confirmation-panel" role="dialog" aria-label="Confirmar intenção"><h2>Registrar intenção de {decision === "approve" ? "aprovar" : "rejeitar"}</h2><p><b>{selected.gate}</b> · responsável {selected.assigned_to} · ETag <span className="mono">"v{selected.version}"</span></p><p>Esta ação ainda não decide a Approval. Ela não libera nem executa automaticamente a Task.</p><textarea aria-label="Motivo da decisão" maxLength={10000} value={reason} onChange={(event) => setReason(event.target.value)} />{actionError && <p role="alert">{actionError}</p>}<button disabled={submitting} onClick={register}>{submitting ? "Registrando…" : "Registrar intenção"}</button><button onClick={() => setSelected(null)}>Cancelar</button></section>}
    {intent && <section className="confirmation-panel success" role="status"><h2>Intenção registrada — decisão ainda pendente</h2><p>Confirme em um terminal confiável:</p><code>agentbus-approval-confirm confirm {intent.intent_id}</code><p>Nenhuma credencial humana foi enviada ao navegador.</p></section>}
  </>;
}
