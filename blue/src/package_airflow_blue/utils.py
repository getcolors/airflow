CONTRACT=1
def host_alias(o): return str(o.get("profile") or "airflow")
def registrable_domain(h):
 p=str(h).split(".");return str(h) if len(p)<2 else ".".join(p[-2:])
def once_applications(o): return {"applications":[{"host":o.get("airflow-host")}]}
def with_once_shape(o): return {**o,"once":once_applications(o)}
