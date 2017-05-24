package cqelsplus.execplan.data;

import com.hp.hpl.jena.sparql.core.Var;

public class AccMax implements Accumulator {
	Var var;
	
	public AccMax() {}
	public void setAccVar(Var var) {
		this.var = var;
	}

	public AccMax(Var var) {
		this.var=var;
	}

	
	public Var accVar() {
		// TODO Auto-generated method stub
		return var;
	}

	public DomAggEntry aggEntry() {
		// TODO Auto-generated method stub
		return (DomMinEntry)POOL.DomMinEntry.borrowObject();
	}

}
