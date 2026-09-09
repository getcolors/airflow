import {expect,test} from 'bun:test';
import {readFileSync} from 'node:fs';
import * as machine from '../src/machine.ts';
import * as tools from '../src/tools.ts';
import * as github from '../src/github.ts';
import {stateErrors,secretErrors} from '../src/validate.ts';
const fixture=()=>Bun.YAML.parse(readFileSync(new URL('../../test/fixtures/colors.yml',import.meta.url),'utf8')) as any;
test('firewall and legacy state are explicit',()=>{
 const opts=fixture(),req=machine.requirements(opts);
 expect(req.security.ingress.map(r=>r.from_port)).toEqual([22,80,443]);
 expect(req.legacy_state_keys).toEqual(['airflow-fixture/airflow-compute.tfstate']);
 for(const override of [{'digitalocean-firewall':false},{'provider-compute':'no-infra'},{'provider-backend':'local'}])expect(stateErrors({...opts,...override}).length).toBeGreaterThan(0);
});
test('returned identity and login reach inventory and host key probe',()=>{
 const opts=fixture(),node={name:opts.profile,ip:'203.0.113.7',user:'ubuntu',provider_id:'immutable-id'};
 for(const mode of ['managed','external']){
  const selected={...opts};if(mode==='managed')delete selected['digitalocean-ssh-keys'];
  const adopted=machine.params(selected,{status:'ready',cluster:{nodes:[node]},key:{private_key_path:'/tmp/test-key'}});
  expect(adopted['ssh-keygen']).toBe(mode==='managed');
  const data=tools.dataFn({...opts,...adopted});
  expect(JSON.parse(tools.inventory(data)).all.hosts[opts.profile]).toEqual({ansible_host:node.ip,ansible_user:node.user,ansible_ssh_private_key_file:'/tmp/test-key'});
  const args=github.hostKeyArgs(data);expect(args[args.indexOf('-i')+1]).toBe('/tmp/test-key');expect(args).toContain('ubuntu@203.0.113.7');
 }
});
test('credentials follow the shared library and application',()=>{
 const errors=secretErrors({...fixture(),'provider-backend':'r2'}).join('\n');
 for(const variable of ['COLORS_PAR_DO_TOKEN','COLORS_PAR_R2_ACCESS_KEY_ID','COLORS_PAR_GITHUB_TOKEN'])expect(errors).toContain(variable);
 expect(errors).not.toContain('COLORS_PAR_AWS_ACCESS_KEY_ID');
});
