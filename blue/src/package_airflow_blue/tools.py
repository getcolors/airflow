import json,re
from pathlib import Path
from importlib.resources import files
from blue import tofu
from blue.ansible import ansible_step,ansible_with_spec
from blue.cli import par_name,stage_dir
from blue.providers import tool_env
from blue.scaffold import PRESERVE_JINJA_DELIMITERS,content_spec,scaffold
from package_once_blue import tools as once_tools
from . import github, machine
from .utils import host_alias,with_once_shape
from .validate import providers
ROOT=Path(__file__).parent/"resources";OPTS=PRESERVE_JINJA_DELIMITERS
def template(p):return {"name":p,"content":str((ROOT/p).read_text())}
def spec(t,target,data):return {"template":t,"target":target,"data":data,"opts":OPTS}
def raw_spec(target,content):return content_spec(target,content)
compute_tool="airflow-compute";ansible_local_tool="airflow-ansible-local";ansible_remote_tool="airflow-ansible-remote";github_tool="airflow-github";dns_tool="tofu-dns";smtp_tool="tofu-smtp";smtp_post_tool="tofu-smtp-post"
def tool_dir(o,t):return stage_dir(o,t,default_profile="airflow")
def delegated_tool_dir(o,t):return once_tools.tool_dir(o,t)
def credential_env(o,*slots):return tool_env(providers,o,[*slots,"provider-backend"])
def backend_credential_env(o):return credential_env(o)
fallback_compute_params=machine.fallback_params
compute_step=machine.step
async def smtp_step(o):return await once_tools.tofu_smtp_step(with_once_shape(o))
async def dns_step(o):return await once_tools.tofu_dns_step(with_once_shape(o))
async def smtp_post_step(o):return await once_tools.tofu_smtp_post_step(with_once_shape(o))
def par_lookup(k):return"{{ lookup('env','COLORS_PAR_"+k.upper().replace("-","_")+"') }}"
def data_fn(o):
 no=o.get("provider-smtp")=="no-infra";relay={"smtp-server":o.get("no-infra-smtp-server"),"smtp-port":o.get("no-infra-smtp-port"),"smtp-username":o.get("no-infra-smtp-username")}if no else{"smtp-server":"smtp.resend.com","smtp-port":587,"smtp-username":"resend"};pk="no-infra-smtp-password"if no else"resend-password";v=str(o.get("postgres-version"));return{**o,**relay,"ip":str(o.get("ip")or""),"user":str(o.get("user")or"root"),"host-alias":host_alias(o),"deploy-user":"deploy","airflow-conf-dir":"/etc/airflow","airflow-logs-dir":"/var/lib/airflow/logs","airflow-uid":"50000","walg-conf-dir":"/etc/wal-g.d","walg-env-file":"/etc/wal-g.d/env","walg-prefix":f"s3://{o.get('walg-r2-bucket')}/{o.get('profile')or'airflow'}","rrsync-path":"/usr/local/bin/rrsync","docker-bridge-gateway":"172.17.0.1","pgdata":f"/var/lib/postgresql/{v}/main","postgres-conf-dir":f"/etc/postgresql/{v}/main","smtp-password-par":par_name(pk),"smtp-password-lookup":par_lookup(pk)}
def deploy_keys_content(o):
 ls=[f'restrict,command="/usr/local/bin/rrsync -wo {o.get("dags-dest")}" {k["public"]}'for k in github.public_keys(o)];return"".join(x+"\n"for x in ls)
def pretty(v,i=0):
 if isinstance(v,list):return"[ ]"if not v else"[ "+", ".join(pretty(x,i)for x in v)+" ]"
 if isinstance(v,dict):return"{ }"if not v else"{\n"+",\n".join(" "*(i+2)+json.dumps(str(k))+" : "+pretty(x,i+2)for k,x in v.items())+"\n"+" "*i+"}"
 return json.dumps(v)
def inventory(d):return pretty({"all":{"hosts":{d.get("host-alias")or"airflow":{"ansible_host":d.get("ip"),"ansible_user":d.get("user"),**({"ansible_ssh_private_key_file":d["ssh-private-key-path"]} if d.get("ssh-private-key-path") else {})}}}})
SEED=[{"path":".github/workflows/deploy-dags.yml","template":"tools/github/deploy-dags.yml"},{"path":"dags/hello_world.py","template":"tools/github/hello_world.py"}]
def seed_step(o):
 d=tool_dir(o,github_tool);r=scaffold(o,[spec(template(x["template"]),f'{d}/seed/{x["path"]}',data_fn(o))for x in SEED])
 if(r.get("blue/exit")or 0)>0 or o.get("blue/event")=="delete":return r
 return{**r,"airflow/seed-files":[{"path":x["path"],"content":Path(f'{d}/seed/{x["path"]}').read_text()}for x in SEED]}
async def ansible_local_step(o):
 d=tool_dir(o,ansible_local_tool);x=data_fn(o);ss=[spec(template(f"tools/ansible-local/{n}"),f"{d}/{n}",x)for n in["ansible.cfg","inventory.ini","main.yml"]];return await ansible_with_spec(o,ss,dir=d,inventory="inventory.ini",playbooks={"create":"main.yml","delete":"main.yml"},extra_vars={"ssh_legacy_marker_prefix":"airflow","host_alias":x["host-alias"],"ssh_hosts":[{"name":x["host-alias"],"ip":x["ip"],"user":x["user"],"identity_file":o.get("ssh-private-key-path")}],"colors_keygen":bool(o.get("ssh-keygen")),"block_state":"absent"if o.get("blue/event")=="delete"else"present"})
FILES=["authorized-keys","docker-compose.yml","Caddyfile","wal-g-wrapper","walg-basebackup","walg-check","walg-notify"]
def ansible_remote_specs(o,d):
 x=data_fn(o);return[spec(template("tools/ansible-remote/ansible.cfg"),f"{d}/ansible.cfg",x),spec(template("tools/ansible-remote/main.yml"),f"{d}/main.yml",x),raw_spec(f"{d}/inventory.json",inventory(x)),raw_spec(f"{d}/deploy_keys",deploy_keys_content(o)),*[spec(template(f"tools/ansible-remote/files/{n}"),f"{d}/files/{n}",x)for n in FILES]]
async def ansible_remote_step(o):
 d=tool_dir(o,ansible_remote_tool);r=scaffold(o,ansible_remote_specs(o,d));return r if o.get("blue/event")in("build","delete")else await ansible_step(r,dir=d,inventory="inventory.json",playbooks={"create":"main.yml"},host_key_checking=False)
