from package_airflow_blue.github import key_comment,known_hosts_line,placeholder_keys
from package_airflow_blue.tools import cidr_list,fallback_compute_params
from package_airflow_blue.validate import env_errors,state_errors
def test_deploy_key_is_deterministic():
 o={"profile":"p","dags-repo":"acme/dags"};assert key_comment(o)=="airflow-deploy-p-acme-dags";assert len(placeholder_keys(o))==1
def test_host_key_parser():assert known_hosts_line("1.2.3.4","ssh-ed25519 AAAA host")=="1.2.3.4 ssh-ed25519 AAAA"
def test_cidrs_accept_overlay_strings():assert cidr_list({"x":"10/8, 20/8"},"x")==["10/8","20/8"]
def test_fallback_name():assert fallback_compute_params({"profile":"p","provider-compute":"digitalocean"})["name"]=="p"
def test_profile_env_refused():assert env_errors({"COLORS_PAR_PROFILE":"other"})
def test_validation_accumulates():assert len(state_errors({}))>10
