import {test,expect} from "bun:test";
import {wireFn} from "../src/workflow.ts";
for(const event of ["create","build"])test(`SSH alias precedes remote convergence on ${event}`,()=>{
 const opts={"red/event":event};
 expect(wireFn("airflow/smtp-post",opts)?.slice(1)).toEqual(["airflow/ansible-local"]);
 expect(wireFn("airflow/ansible-local",opts)?.slice(1)).toEqual(["airflow/ansible-remote"]);
});
