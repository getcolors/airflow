import asyncio,sys
from blue.cli import find_up,run_cli
from .workflow import airflow_workflow
USAGE="Usage: blue <build|create|delete> [-f|--file colors.yml] [--dry-run]"
def find():return find_up("colors.yml")or"colors.yml"
def default_args(a):return a if any(x in("-f","--file")or x.startswith("--file=")for x in a)else[*a,"-f",find()]
async def run(*a):
 a=default_args(list(a));c=a[0]if a else None
 if c in("help","--help","-h"):return{"blue/exit":0,"blue/err":USAGE}
 if c in("build","create","delete"):return await run_cli(airflow_workflow,a)
 return{"blue/exit":2,"blue/err":USAGE}
def exec(args=None):
 r=asyncio.run(run(*(sys.argv[1:]if args is None else args)))
 if r.get("blue/err"):print(r["blue/err"],file=sys.stdout if(r.get("blue/exit")or 0)==0 else sys.stderr)
 raise SystemExit(r.get("blue/exit")or 0)
