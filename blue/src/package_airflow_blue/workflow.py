import os
from pathlib import Path
from blue import dry_run,progress,tofu
from blue.cli import par_name,read_pars
from blue.workflow import advice_add,workflow
from . import github,tools
from .validate import env_errors,secret_errors,state_errors
DEFAULTS={"compute-prevent-destroy":True,"provider-compute":"digitalocean","provider-dns":"cloudflare","provider-smtp":"resend","provider-backend":"local","workdir":".colors"}
async def state_output(o,d):
 try:return(await tofu.outputs(d,tools.backend_credential_env(o))).get("params")
 except:return None
async def adopt_existing_state(o):
 c=await state_output(o,tools.tool_dir(o,tools.compute_tool));s=await state_output(o,tools.delegated_tool_dir(o,tools.smtp_tool));return{**o,**(c or{}),**(s or{}),**({"once/compute-params":c}if c else{}),**({"once/smtp-params":s}if s else{})}
async def with_keys(o,real):
 if real and o.get("blue/event")=="create":
  k,e=await github.generate_keys(o)
  if e:return{**o,"blue/exit":1,"blue/err":e}
  return{**o,"blue/exit":0,"airflow/deploy-keys":k,**({"airflow/key-dir":str(Path(k[0]["private-file"]).parent)}if k else{})}
 return{**o,"blue/exit":0,"airflow/deploy-keys":github.placeholder_keys(o)}
async def start_step(original,env=None):
 env=os.environ if env is None else env;o=read_pars({**DEFAULTS,**original},env);ev=o.get("blue/event");real=not o.get("blue/dry-run");life=ev in("create","delete");es=[*env_errors(env),*state_errors(o),*(secret_errors(o)if real and life else[])]
 if real and ev=="delete"and o.get("compute-prevent-destroy"):es.append(f"compute destruction is protected; set {par_name('compute-prevent-destroy')}=false to delete")
 if es:return{**o,"blue/exit":2,"blue/err":"\n".join(es)}
 return{**(await adopt_existing_state(o)),"blue/exit":0}if real and ev=="delete"else await with_keys(o,real)
async def github_step(o):
 r=tools.seed_step(o);return r if(r.get("blue/exit")or 0)>0 else await github.github_step(r)
async def ansible_cleanup_step(o):return await tools.ansible_remote_step(await tools.ansible_local_step(o))
def wire_fn(s,o):
 if o.get("blue/event")=="delete":return{"airflow/start":(start_step,"airflow/github"),"airflow/github":(github_step,"airflow/ansible-cleanup"),"airflow/ansible-cleanup":(ansible_cleanup_step,"airflow/smtp-post"),"airflow/smtp-post":(tools.smtp_post_step,"airflow/dns"),"airflow/dns":(tools.dns_step,"airflow/smtp","airflow/compute"),"airflow/smtp":(tools.smtp_step,),"airflow/compute":(tools.compute_step,)}.get(s)
 return{"airflow/start":(start_step,"airflow/compute"),"airflow/compute":(tools.compute_step,"airflow/smtp"),"airflow/smtp":(tools.smtp_step,"airflow/dns"),"airflow/dns":(tools.dns_step,"airflow/smtp-post"),"airflow/smtp-post":(tools.smtp_post_step,"airflow/ansible-local","airflow/ansible-remote"),"airflow/ansible-local":(tools.ansible_local_step,),"airflow/ansible-remote":(tools.ansible_remote_step,"airflow/github"),"airflow/github":(github_step,)}.get(s)
def backend_advice(dir_fn,t):
 key=lambda o:f"{o.get('profile')or'airflow'}/{t}.tfstate";return tofu.backends(lambda o:str(o.get("provider-backend")or"local"),{"local":tofu.local_backend_advice(dir_fn),"s3":tofu.s3_backend_advice(dir_fn,lambda o:{"bucket":o.get("s3-bucket"),"key":key(o),"region":o.get("s3-region")}),"r2":tofu.r2_backend_advice(dir_fn,lambda o:{"bucket":o.get("r2-bucket"),"key":key(o),"endpoint":o.get("r2-endpoint")})})
SIDE=["airflow/compute","airflow/smtp","airflow/dns","airflow/smtp-post","airflow/ansible-local","airflow/ansible-remote","airflow/ansible-cleanup","airflow/github"]
def create_workflow():
 w=workflow(start="airflow/start",wire_fn=wire_fn)
 for t,step,own in[(tools.compute_tool,"compute",True),(tools.smtp_tool,"smtp",False),(tools.dns_tool,"dns",False),(tools.smtp_post_tool,"smtp-post",False)]:
  d=(lambda o,t=t:tools.tool_dir(o,t))if own else(lambda o,t=t:tools.delegated_tool_dir(o,t));w=advice_add(w,f"airflow/{step}","before","airflow.workflow/backend",backend_advice(d,t))
 return dry_run.advise(progress.advise(w),SIDE)
airflow_workflow=create_workflow()
