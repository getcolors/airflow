import json
from pathlib import Path
from unittest.mock import AsyncMock
import pytest
import yaml
from package_airflow_blue import machine,tools,github,workflow
from package_airflow_blue.validate import state_errors,secret_errors

def fixture(**overrides):
    return {**yaml.safe_load((Path(__file__).parents[2]/"test/fixtures/colors.yml").read_text()),**overrides}

def test_firewall_and_legacy_state_are_explicit():
    req=machine.requirements(fixture())
    assert [r['from_port'] for r in req['security']['ingress']]==[22,80,443]
    assert req['legacy_state_keys']==['airflow-fixture/airflow-compute.tfstate']
    assert state_errors(fixture(**{'digitalocean-firewall':False}))
    assert state_errors(fixture(**{'provider-compute':'no-infra'}))
    assert state_errors(fixture(**{'provider-backend':'local'}))

@pytest.mark.asyncio
async def test_runtime_inventory_reaches_dns_and_ansible(monkeypatch):
    node={'name':'airflow-fixture','ip':'203.0.113.7','user':'ubuntu','provider_id':'immutable-id'}
    monkeypatch.setattr(machine,'orchestrate',AsyncMock(return_value={'status':'ready','cluster':{'nodes':[node]},'key':{'private_key_path':'/tmp/test-key'}}))
    opts=fixture(**{'blue/event':'create'})
    opts.pop('digitalocean-ssh-keys')
    result=await machine.step(opts)
    assert result['ssh-keygen'] is True
    assert result['once/compute-params']['ip']==node['ip']
    assert result['colors-compute/cluster']['nodes'][0]['provider_id']=='immutable-id'
    inventory=json.loads(tools.inventory(tools.data_fn(result)))['all']['hosts']['airflow-fixture']
    assert inventory=={'ansible_host':node['ip'],'ansible_user':'ubuntu','ansible_ssh_private_key_file':'/tmp/test-key'}
    args=github.host_key_args(result)
    assert args[args.index('-i')+1]=='/tmp/test-key'
    assert 'ubuntu@203.0.113.7' in args

@pytest.mark.asyncio
async def test_inventory_failure_stops_delete_before_smtp(monkeypatch):
    monkeypatch.setattr(machine,'load',AsyncMock(return_value={'blue/exit':1,'blue/err':'missing inventory'}))
    read=AsyncMock(side_effect=AssertionError('must not read SMTP state'))
    monkeypatch.setattr(workflow,'state_output',read)
    assert (await workflow.adopt_existing_state(fixture()))['blue/exit']==1
    read.assert_not_called()

@pytest.mark.asyncio
async def test_failed_ssh_cleanup_stops_remote_cleanup(monkeypatch):
    monkeypatch.setattr(tools,'ansible_local_step',AsyncMock(return_value={'blue/exit':1}))
    remote=AsyncMock(side_effect=AssertionError('must not continue'))
    monkeypatch.setattr(tools,'ansible_remote_step',remote)
    assert (await workflow.ansible_cleanup_step({}))['blue/exit']==1
    remote.assert_not_called()

@pytest.mark.asyncio
async def test_local_step_supplies_canonical_host_alias(monkeypatch,tmp_path):
    runner=AsyncMock(return_value={})
    monkeypatch.setattr(tools,'ansible_with_spec',runner)
    await tools.ansible_local_step(fixture(workdir=str(tmp_path),ip='203.0.113.7',user='ubuntu',**{'blue/event':'build','ssh-keygen':True}))
    variables=runner.call_args.kwargs['extra_vars']
    assert variables['host_alias']=='airflow-fixture'
    assert variables['ssh_hosts'][0]['name']==variables['host_alias']

def test_credentials_follow_library_and_named_repository():
    errors='\n'.join(secret_errors(fixture(**{'provider-backend':'r2'})))
    for variable in ['COLORS_PAR_DO_TOKEN','COLORS_PAR_R2_ACCESS_KEY_ID','COLORS_PAR_GITHUB_TOKEN']:
        assert variable in errors
    assert 'COLORS_PAR_AWS_ACCESS_KEY_ID' not in errors
