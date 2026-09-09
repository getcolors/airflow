import pytest
from blue.workflow import run, workflow as graph
from package_airflow_blue.workflow import wire_fn

@pytest.mark.parametrize("event", ["create", "build"])
@pytest.mark.parametrize("failure", [False, True])
async def test_alias_update_finishes_before_remote_convergence(event, failure):
    seen=[]
    opts={"blue/event":event}
    assert wire_fn("airflow/smtp-post",opts)[1:]==("airflow/ansible-local",)
    assert wire_fn("airflow/ansible-local",opts)[1:]==("airflow/ansible-remote",)
    def wire(step, run_opts):
        original=wire_fn(step,run_opts)
        async def fake(current):
            seen.append(step)
            return {**current,"blue/exit":1 if failure and step=="airflow/ansible-local" else 0}
        return (fake, *original[1:]) if original else None
    result=await run(graph(start="airflow/smtp-post",wire_fn=wire),opts)
    assert seen[:2]==["airflow/smtp-post","airflow/ansible-local"]
    assert ("airflow/ansible-remote" in seen) is not failure
    assert result["blue/exit"]==(1 if failure else 0)
