import re
from blue.cli import par_name
from package_once_blue.validate import providers
slots=["provider-compute","provider-smtp","provider-dns","provider-backend"]
own_required=["airflow-host","airflow-image","airflow-admin-username","caddy-image","airflow-smtp-from","dags-repo","dags-dest","dags-branch","postgres-version","walg-version","walg-r2-bucket","walg-r2-endpoint","walg-r2-region","walg-full-backup-oncalendar","walg-retain-full","walg-max-backup-age-hours","alerts-email"]
own_secrets=["github-token","postgres-password","airflow-fernet-key","airflow-admin-password","walg-r2-access-key-id","walg-r2-secret-access-key"]
def entry(o,s):return providers.get(s,{}).get(str(o.get(s)),{})
def tofu_env(o,s):return entry(o,s).get("tofu-env",{})
def keys(o,f):return [k for s in slots for k in entry(o,s).get(f,[])]
def placeholder(x):return x is None or isinstance(x,str) and (not x.strip() or x.upper()=="REPLACE_ME")
def env_errors(env):return ["COLORS_PAR_PROFILE is set. This package takes its profile from colors.yml only — run from the project directory rather than overriding it."] if env.get("COLORS_PAR_PROFILE") else []
def state_errors(o):
 e=[]
 for k in ["profile","workdir",*own_required,*keys(o,"required")]:
  if placeholder(o.get(k)):e.append(f"{k} is required")
 for s in slots:
  if o.get(s) not in providers[s]:e.append(f"unsupported {s} {o.get(s)!r}")
 for k in ["caddy-acme-email","digitalocean-vpc-uuid","oci-image-id"]:
  if k in o and o.get(k) is not None and str(o[k]).strip() and placeholder(o[k]):e.append(f"{k} still says REPLACE_ME — fill it in, or delete the key. An optional key is not treated as absent while it holds a placeholder: it renders into the generated files verbatim.")
 if not isinstance(o.get("compute-prevent-destroy"),bool):e.append("compute-prevent-destroy must be true or false")
 if not placeholder(o.get("airflow-host")) and not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+",str(o["airflow-host"])):e.append("airflow-host must be a fully qualified hostname")
 if not placeholder(o.get("dags-repo")) and not re.fullmatch(r"[A-Za-z0-9._-]+/[A-Za-z0-9._-]+",str(o["dags-repo"])):e.append("dags-repo must be owner/name")
 if not placeholder(o.get("dags-dest")) and not re.fullmatch(r"/\S*",str(o["dags-dest"])):e.append("dags-dest must be an absolute path")
 for k,l in [("alerts-email","alert"),("airflow-smtp-from","sender")]:
  if not placeholder(o.get(k)) and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+",str(o[k])):e.append(f"{k} must be an email address (the {l} address)")
 for k,msg in [("airflow-image","airflow-image must carry an explicit tag — a floating tag makes two creates different deployments, and an Airflow minor upgrade migrates the metadata database"),("caddy-image","caddy-image must carry an explicit tag")]:
  if not placeholder(o.get(k)) and ":" not in str(o[k]):e.append(msg)
 if not isinstance(o.get("postgres-version"),int) or isinstance(o.get("postgres-version"),bool) or o["postgres-version"]<=0:e.append("postgres-version must be a positive integer major version")
 if not placeholder(o.get("walg-version")) and not re.fullmatch(r"v?\d+(?:\.\d+)*(?:[-.][A-Za-z0-9.]+)?",str(o["walg-version"])):e.append('walg-version must be a WAL-G release tag, e.g. "v3.0.8" — it names a GitHub release asset')
 for k in ["walg-retain-full","walg-max-backup-age-hours"]:
  if not isinstance(o.get(k),int) or isinstance(o.get(k),bool) or o[k]<=0:e.append(f"{k} must be a positive integer")
 cal=str(o.get("walg-full-backup-oncalendar") or "")
 if not placeholder(cal) and len(cal.strip().split())==5 and ":" not in cal:e.append('walg-full-backup-oncalendar looks like a crontab line. It is a systemd OnCalendar expression — daily at 02:00 is "*-*-* 02:00:00", not "0 2 * * *"')
 f,h=str(o.get("airflow-smtp-from")),str(o.get("airflow-host"));z=".".join(h.split(".")[-2:])
 if o.get("provider-smtp")=="resend" and not placeholder(f) and not placeholder(h) and re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+",f) and not f.endswith(f"@notifications.{z}"):e.append(f"airflow-smtp-from must be under the Resend sending domain notifications.{z} — that subdomain is what gets verified, not the bare zone")
 return e
def secret_errors(o):return [f"required credential is not set: {par_name(k)}" for k in dict.fromkeys([*own_secrets,*keys(o,'secrets')]) if placeholder(o.get(k))]
