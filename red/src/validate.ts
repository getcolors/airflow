import { parName } from "red/cli";
import type { Opts } from "red/workflow";
import { providers } from "package-once-red";
export { providers };
export const slots=["provider-compute","provider-smtp","provider-dns","provider-backend"];
export const ownRequired=["airflow-host","airflow-image","airflow-admin-username","caddy-image","airflow-smtp-from","dags-repo","dags-dest","dags-branch","postgres-version","walg-version","walg-r2-bucket","walg-r2-endpoint","walg-r2-region","walg-full-backup-oncalendar","walg-retain-full","walg-max-backup-age-hours","alerts-email"];
export const ownSecrets=["github-token","postgres-password","airflow-fernet-key","airflow-admin-password","walg-r2-access-key-id","walg-r2-secret-access-key"];
const entry=(o:Opts,s:string)=>(providers as any)[s]?.[String(o[s])];
export const tofuEnv=(o:Opts,s:string):Record<string,string>=>entry(o,s)?.tofuEnv||{};
const keys=(o:Opts,f:string)=>slots.flatMap(s=>entry(o,s)?.[f]||[]);
export const placeholder=(x:unknown)=>x==null||(typeof x==="string"&&(!x.trim()||x.toUpperCase()==="REPLACE_ME"));
const host=/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/;
const repo=/^[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+$/; const email=/^[^@\s]+@[^@\s]+\.[^@\s]+$/; const release=/^v?\d+(?:\.\d+)*(?:[-.][A-Za-z0-9.]+)?$/;
export function envErrors(env:Record<string,string|undefined>){return env.COLORS_PAR_PROFILE?["COLORS_PAR_PROFILE is set. This package takes its profile from colors.yml only — run from the project directory rather than overriding it."]:[]}
export function stateErrors(o:Opts):string[]{const e:string[]=[];
 for(const k of ["profile","workdir",...ownRequired,...keys(o,"required")])if(placeholder(o[k]))e.push(`${k} is required`);
 for(const s of slots)if(!(providers as any)[s]?.[String(o[s])])e.push(`unsupported ${s} ${JSON.stringify(o[s])}`);
 for(const k of ["caddy-acme-email","digitalocean-vpc-uuid","oci-image-id"])if(k in o&&o[k]!=null&&String(o[k]).trim()&&placeholder(o[k]))e.push(`${k} still says REPLACE_ME — fill it in, or delete the key. An optional key is not treated as absent while it holds a placeholder: it renders into the generated files verbatim.`);
 if(typeof o["compute-prevent-destroy"]!=="boolean")e.push("compute-prevent-destroy must be true or false");
 if(!placeholder(o["airflow-host"])&&!host.test(String(o["airflow-host"])))e.push("airflow-host must be a fully qualified hostname");
 if(!placeholder(o["dags-repo"])&&!repo.test(String(o["dags-repo"])))e.push("dags-repo must be owner/name");
 if(!placeholder(o["dags-dest"])&&!/^\/\S*$/.test(String(o["dags-dest"])))e.push("dags-dest must be an absolute path");
 for(const [k,l] of [["alerts-email","alert"],["airflow-smtp-from","sender"]])if(!placeholder(o[k])&&!email.test(String(o[k])))e.push(`${k} must be an email address (the ${l} address)`);
 if(!placeholder(o["airflow-image"])&&!String(o["airflow-image"]).includes(":"))e.push("airflow-image must carry an explicit tag — a floating tag makes two creates different deployments, and an Airflow minor upgrade migrates the metadata database");
 if(!placeholder(o["caddy-image"])&&!String(o["caddy-image"]).includes(":"))e.push("caddy-image must carry an explicit tag");
 if(!Number.isInteger(o["postgres-version"])||Number(o["postgres-version"])<=0)e.push("postgres-version must be a positive integer major version");
 if(!placeholder(o["walg-version"])&&!release.test(String(o["walg-version"])))e.push('walg-version must be a WAL-G release tag, e.g. "v3.0.8" — it names a GitHub release asset');
 for(const k of ["walg-retain-full","walg-max-backup-age-hours"])if(!Number.isInteger(o[k])||Number(o[k])<=0)e.push(`${k} must be a positive integer`);
 const cal=String(o["walg-full-backup-oncalendar"]??"");if(!placeholder(cal)&&cal.trim().split(/\s+/).length===5&&!cal.includes(":"))e.push('walg-full-backup-oncalendar looks like a crontab line. It is a systemd OnCalendar expression — daily at 02:00 is "*-*-* 02:00:00", not "0 2 * * *"');
 const from=String(o["airflow-smtp-from"]),h=String(o["airflow-host"]);const zone=h.split(".").slice(-2).join(".");if(o["provider-smtp"]==="resend"&&!placeholder(from)&&!placeholder(h)&&email.test(from)&&!from.endsWith(`@notifications.${zone}`))e.push(`airflow-smtp-from must be under the Resend sending domain notifications.${zone} — that subdomain is what gets verified, not the bare zone`);
 return e.map(x=>x.startsWith(":")?x:x.replace(/^(airflow|dags|postgres|walg|alerts|caddy|compute)-/,"$&"));}
export function secretErrors(o:Opts){return [...new Set([...ownSecrets,...keys(o,"secrets")])].filter(k=>placeholder(o[k])).map(k=>`required credential is not set: ${parName(k)}`)}
