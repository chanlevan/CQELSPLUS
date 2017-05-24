package cqelsplus.execplan.data;

import com.hp.hpl.jena.sparql.core.Var;

public interface Accumulator {
	public void setAccVar(Var var);
	public Var accVar();
	public DomAggEntry aggEntry();
}
