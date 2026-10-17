export function Loading({ label = "Consultando o AgentBus…" }: { label?: string }) {
  return <div className="state-card" role="status"><span className="spinner" />{label}</div>;
}

export function ErrorState({ message, retry }: { message: string; retry: () => void }) {
  return <div className="state-card error" role="alert"><strong>Consulta indisponível</strong><p>{message}</p><button onClick={retry}>Tentar novamente</button></div>;
}

export function EmptyState({ title, message }: { title: string; message: string }) {
  return <div className="state-card empty"><strong>{title}</strong><p>{message}</p></div>;
}
