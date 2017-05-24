package cqelsplus.execplan.data;

import com.hp.hpl.jena.sparql.core.Var;

public class AccMin implements Accumulator {
	Var var;
	
	public AccMin() {}
	public void setAccVar(Var var) {
		this.var = var;
	}

	public AccMin(Var var) {
		this.var=var;
	}

	
	public Var accVar() {
		// TODO Auto-generated method stub
		return var;
	}

	public DomAggEntry aggEntry() {
		// TODO Auto-generated method stub
		return (DomAggEntry)POOL.DomMinEntry.borrowObject();
	}

}
