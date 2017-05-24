package cqelsplus.execplan.data;

import com.hp.hpl.jena.sparql.core.Var;

public class AccSum implements Accumulator {
	long sum;
	Var var;
	public AccSum() {}
	
	public void setAccVar(Var var) {
		this.var = var;
	}
	
	public AccSum(Var var) {
		this.var=var;
	}

	
	public DomAggEntry aggEntry() {
		// TODO Auto-generated method stub
		return (DomSumEntry)POOL.DomSumEntry.borrowObject();
	}


	public Var accVar() {
		// TODO Auto-generated method stub
		return var;
	}

}
