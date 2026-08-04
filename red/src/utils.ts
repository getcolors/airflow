import type { Opts } from "red/workflow";
export const contract = 1;
export const hostAlias = (o: Opts) => String(o.profile || "airflow");
export function registrableDomain(host: unknown): string { const p=String(host).split("."); return p.length<2?String(host):p.slice(-2).join("."); }
export const onceApplications = (o: Opts) => ({ applications: [{ host: o["airflow-host"] }] });
export const withOnceShape = (o: Opts): Opts => ({ ...o, once: onceApplications(o) });
