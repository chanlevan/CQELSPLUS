package cqelsplus.execplan.data;

import com.hp.hpl.jena.sparql.core.Var;

public class AccCount implements Accumulator {
	Var var;
	public AccCount() {}
	public AccCount(Var var){
		this.var = var;
	}
	public void setAccVar(Var var) {
		this.var = var;
	}

	public DomAggEntry aggEntry(){
		return (DomAggEntry)POOL.DomAggEntry.borrowObject();
	}

	public Var accVar() {
		return var;
	}
}
