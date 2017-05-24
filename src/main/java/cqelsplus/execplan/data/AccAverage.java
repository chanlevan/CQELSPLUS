package cqelsplus.execplan.data;

import com.hp.hpl.jena.sparql.core.Var;

public class AccAverage implements Accumulator {
	
	Var var;
	public AccAverage() {}
	
	public void setAccVar(Var var) {
		this.var = var;
	}
	
	public AccAverage(Var var) {
		this.var=var;
	}

	public Var accVar() {
		return var;
	}

	public DomAggEntry aggEntry() {
		// TODO Auto-generated method stub
		return (DomAVGEntry)POOL.DomAVGEntry.borrowObject();
	}


}
