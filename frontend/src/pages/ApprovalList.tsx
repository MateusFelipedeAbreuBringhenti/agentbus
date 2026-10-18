import { FormEvent, useCallback, useState } from "react";
import { createApprovalIntent, listApprovals } from "../api";
import { EmptyState, ErrorState, Loading } from "../components/AsyncState";
import { useLoad } from "../hooks/useLoad";
import type { Approval, ApprovalIntent } from "../types";

export function ApprovalList() {
  const [assignee, setAssignee] = useState("");
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<Approval | null>(null);
  const [decision, setDecision] = useState<"approve" | "reject">("approve");
  const [reason, setReason] = useState("");
  const [intent, setIntent] = useState<ApprovalIntent | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const load = useCallback((signal: AbortSignal) => listApprovals({ assignedTo: filter || undefined, status: "pending" }, signal), [filter]);
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
    <section className="page-heading"><div><small>GOVERNANÇA · DECISÃO HUMANA</small><h1>Aprovações pendentes</h1><p>O painel registra intenções. Somente o confirmador humano autenticado decide.</p></div><span className="readonly-pill">SEM CREDENCIAL HUMANA</span></section>
    <div className="warning-banner"><strong>Decisão ≠ reação</strong> Aprovar ou rejeitar não libera nem executa automaticamente a Task.</div>
    <form className="filters" onSubmit={apply} aria-label="Filtros de aprovações"><input aria-label="Responsável" placeholder="Responsável" value={assignee} onChange={(event) => setAssignee(event.target.value)} /><button type="submit">Aplicar filtro</button></form>
    {loading && <Loading />}{error && <ErrorState message={error} retry={retry} />}
    {!loading && !error && data?.items.length === 0 && <EmptyState title="Nenhuma Approval pendente" message="O AgentBus respondeu normalmente para estes filtros." />}
    {!loading && !error && data && data.items.length > 0 && <section className="approval-grid">{data.items.map((approval) => <article className="approval-card" key={approval.id}><small>{approval.gate} · v{approval.version}</small><h2>{approval.request_reason}</h2><p>Responsável: <b>{approval.assigned_to}</b></p><p>Task: <span className="mono">{approval.task_id}</span></p><div className="approval-actions"><button onClick={() => prepare(approval, "approve")}>Solicitar aprovação</button><button className="danger" onClick={() => prepare(approval, "reject")}>Solicitar rejeição</button></div></article>)}</section>}
    {selected && !intent && <section className="confirmation-panel" role="dialog" aria-label="Confirmar intenção"><h2>Registrar intenção de {decision === "approve" ? "aprovar" : "rejeitar"}</h2><p><b>{selected.gate}</b> · responsável {selected.assigned_to} · ETag <span className="mono">"v{selected.version}"</span></p><p>Esta ação ainda não decide a Approval. Ela não libera nem executa automaticamente a Task.</p><textarea aria-label="Motivo da decisão" maxLength={10000} value={reason} onChange={(event) => setReason(event.target.value)} />{actionError && <p role="alert">{actionError}</p>}<button disabled={submitting} onClick={register}>{submitting ? "Registrando…" : "Registrar intenção"}</button><button onClick={() => setSelected(null)}>Cancelar</button></section>}
    {intent && <section className="confirmation-panel success" role="status"><h2>Intenção registrada — decisão ainda pendente</h2><p>Confirme em um terminal confiável:</p><code>agentbus-approval-confirm confirm {intent.intent_id}</code><p>Nenhuma credencial humana foi enviada ao navegador.</p></section>}
  </>;
}
