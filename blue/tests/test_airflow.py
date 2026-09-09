from package_airflow_blue.github import key_comment,known_hosts_line,placeholder_keys
from package_airflow_blue.tools import fallback_compute_params
from package_airflow_blue.validate import env_errors,state_errors
def test_deploy_key_is_deterministic():
 o={"profile":"p","dags-repo":"acme/dags"};assert key_comment(o)=="airflow-deploy-p-acme-dags";assert len(placeholder_keys(o))==1
def test_host_key_parser():assert known_hosts_line("1.2.3.4","ssh-ed25519 AAAA host")=="1.2.3.4 ssh-ed25519 AAAA"
def test_missing_live_inventory_is_refused():
 import pytest
 with pytest.raises(ValueError):fallback_compute_params({"blue/event":"create"})

def test_profile_env_refused():assert env_errors({"COLORS_PAR_PROFILE":"other"})
def test_validation_accumulates():assert len(state_errors({}))>10
