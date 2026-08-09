import shutil,tempfile
from pathlib import Path
from blue.process import run_plan
from blue.runtime import runtime
async def default_run(a,env=None,timeout_ms=30000):return await runtime.exec(a,env=env,timeout_ms=timeout_ms)
PLACE="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBUILDPLACEHOLDER0000000000000000000000"
def repo(o):return str(o.get("dags-repo")or"").strip()or None
def key_comment(o):return f"airflow-deploy-{o.get('profile')or'default'}-{repo(o).replace('/','-')}"
def placeholder_keys(o):return[{"github":repo(o),"public":f"{PLACE} {key_comment(o)}"}]if repo(o)else[]
def public_keys(o):return[{"public":k["public"]}for k in o.get("airflow/deploy-keys")or[]]
async def generate_keys(o,run=None):
 if not repo(o):return[],None
 run=run or default_run;d=tempfile.mkdtemp(prefix="airflow-deploy");p=str(Path(d)/"key-0");x=await run(["ssh-keygen","-t","ed25519","-N","","-q","-C",key_comment(o),"-f",p])
 return([] ,f"ssh-keygen failed for {repo(o)}: {str(x.err).strip()}")if x.exit else([{"github":repo(o),"public":Path(p+".pub").read_text().strip(),"private-file":p}],None)
def host_key_args(o):return["ssh","-o","BatchMode=yes","-o","ConnectTimeout=10","-o","StrictHostKeyChecking=accept-new",f"{o.get('user')or'root'}@{o.get('ip')}","cat /etc/ssh/ssh_host_ed25519_key.pub"]
def known_hosts_line(ip,pub):
 p=str(pub or"").strip().split();return f"{ip} {p[0]} {p[1]}"if len(p)>1 and p[0].startswith("ssh-")else None
async def fetch_host_key(o,run):
 x=await run(host_key_args(o));return known_hosts_line(o.get("ip"),x.out)if x.exit==0 else None
def quote(x):return"'"+str(x).replace("'","'\\''")+"'"
def create_repo_commands(o):return[{"label":f"create {repo(o)}","args":["gh","repo","create",repo(o),"--private","--add-readme","--description",f"Airflow DAGs for {o.get('airflow-host')}, deployed by colors"]}]
def publish_commands(o,k):
 g,e=k["github"],str(o.get("profile")or"default");b=["--repo",g,"--env",e];return[{"label":f"{g} environment {e}","args":["gh","api","--method","PUT","--silent",f"repos/{g}/environments/{e}"]},{"label":f"{g} SERVER_IP","args":["gh","variable","set","SERVER_IP",*b,"--body",str(o.get("ip"))]},{"label":f"{g} SERVER_USER","args":["gh","variable","set","SERVER_USER",*b,"--body","deploy"]},{"label":f"{g} SSH_KNOWN_HOSTS","args":["gh","variable","set","SSH_KNOWN_HOSTS",*b,"--body",str(o.get("airflow/known-hosts")or"")]},{"label":f"{g} SSH_PRIVATE_KEY","args":["sh","-c",f"gh secret set SSH_PRIVATE_KEY --repo {quote(g)} --env {quote(e)} < {quote(k.get('private-file'))}"]}]
def revoke_commands(o):
 g,e=repo(o),str(o.get("profile")or"default");b=["--repo",g,"--env",e];return[{"label":f"{g} {n}","args":["gh","variable","delete",n,*b]}for n in["SERVER_IP","SERVER_USER","SSH_KNOWN_HOSTS"]]+[{"label":f"{g} SSH_PRIVATE_KEY","args":["gh","secret","delete","SSH_PRIVATE_KEY",*b]}]
async def repo_exists(o,run):return(await run(["gh","repo","view",repo(o),"--json","name"],env={"GH_TOKEN":str(o.get("github-token"))})).exit==0
async def seed_repo(o,fs,run):
 if not fs:return None
 d=tempfile.mkdtemp(prefix="airflow-seed")
 try:
  x=await run(["git","clone","--depth","1",f"git@github.com:{repo(o)}.git",d],timeout_ms=120000)
  if x.exit:return f"could not clone git@github.com:{repo(o)}.git over SSH: {str(x.err).strip()}\nThe seed is pushed with your own SSH key rather than the GitHub token, so this needs an SSH key GitHub accepts.\nCheck `ssh -T git@github.com`."
  missing=[f for f in fs if not(Path(d)/f["path"]).exists()]
  if not missing:return None
  for f in missing:
   p=Path(d)/f["path"];p.parent.mkdir(parents=True,exist_ok=True);p.write_text(f["content"])
  commands=[["git","-C",d,"add","--",*[f["path"]for f in missing]],["git","-C",d,"-c","user.name=colors airflow","-c","user.email=airflow@getcolors.ai","commit","-m","Seed the deploy workflow and an example DAG"],["git","-C",d,"push","origin",f"HEAD:{o.get('dags-branch')or'main'}"]]
  labels=["git add failed","git commit failed",f"could not push the seed to {o.get('dags-branch')or'main'}"]
  for a,l in zip(commands,labels):
   x=await run(a,timeout_ms=120000)
   if x.exit:return f"{l}: {str(x.err).strip()}"
 finally:shutil.rmtree(d,ignore_errors=True)
def cleanup(o):
 if isinstance(o.get("airflow/key-dir"),str):shutil.rmtree(o["airflow/key-dir"],ignore_errors=True)
 return{**{k:v for k,v in o.items()if k!="airflow/key-dir"},"airflow/deploy-keys":[{k:v for k,v in x.items()if k!="private-file"}for x in o.get("airflow/deploy-keys")or[]]}
async def github_step(o,run=None):
 run=run or default_run;ev=o.get("blue/event");delete=ev=="delete"
 if ev not in("create","delete")or not repo(o):return o
 if not delete:o={**o,"airflow/known-hosts":await fetch_host_key(o,run)}
 cmds=revoke_commands(o)if delete else([*([]if await repo_exists(o,run)else create_repo_commands(o)),*[c for k in o.get("airflow/deploy-keys")or[]for c in publish_commands(o,k)]])
 result=await run_plan(cmds,runner=lambda c:run(c["args"],env={"GH_TOKEN":str(o.get("github-token"))}),continue_on_error=lambda *_:delete)
 fail=f"gh failed for {result['command']['label']}: {str(result['err']).strip()}"if result["exit"]else None
 if not delete and not fail:fail=await seed_repo(o,o.get("airflow/seed-files")or[],run)
 z=cleanup(o);return{**z,"blue/exit":1,"blue/err":fail}if fail else{**z,"blue/exit":0}
