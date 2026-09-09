import {test,expect,spyOn} from "bun:test";
import * as library from "colors-compute-red";
import * as machine from "../src/machine.ts";
import {wireFn,nextFn} from "../src/workflow.ts";
import {run,workflow,type Opts} from "red/workflow";
for(const status of ['destroyed','absent','error']) for(const event of ['create','delete']) test(`inventory ${status} on ${event}`,async()=>{
 const read=spyOn(library,'read_deployment').mockResolvedValue({status} as any);
 try {const result=await machine.load({'red/event':event});const allowed=status==='destroyed'&&event==='delete';expect(result['red/exit']).toBe(allowed?0:1);expect(Boolean(result['colors-compute/already-destroyed'])).toBe(allowed);}finally{read.mockRestore();}
});
for(const retired of [true,false])for(const failure of [true,false])test(`native delete retired=${retired} failure=${failure}`,async()=>{
 const seen:string[]=[];
 const wf=workflow({start:'airflow/start',nextFn,wireFn:(step,opts)=>{
  const original=wireFn(step,opts);
  return original?[async(current:Opts)=>{seen.push(step);return {...current,'colors-compute/already-destroyed':retired,'red/exit':failure?1:0};},...original.slice(1)] as any:null;
 }});
 const result=await run(wf,{'red/event':'delete'});
 if(retired||failure)expect(seen).toEqual(['airflow/start']);else expect(seen.slice(-3)).toEqual(['airflow/dns','airflow/smtp','airflow/compute']);
 expect(result['red/exit']).toBe(failure?1:0);
});
