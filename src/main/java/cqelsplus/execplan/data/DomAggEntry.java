package cqelsplus.execplan.data;


import com.hp.hpl.jena.sparql.expr.NodeValue;

import cqelsplus.engine.ExecContextFactory;



public class DomAggEntry extends DomEntry {
	protected Accumulator acc;
	public DomAggEntry() {}
	public void setAcc(Accumulator acc){
		this.acc=acc;
	}

	public Accumulator getAcc() {
		return this.acc;
	}
	
	public void update(IMapping mu){
		incCount();
	}
	
	public void expire(IMapping mu) { 
		decrCount(); 
	}

	public long accVal() {
		NodeValue countNV = NodeValue.makeInteger(count());
		return ExecContextFactory.current().engine().encode(countNV.asNode());
	}
	
	public PoolableObject newObject() {
		return (PoolableObject)POOL.DomAggEntry.borrowObject();
	}
	
	public void releaseInstance() {
		POOL.DomAggEntry.returnObject(this);
	}
	public void reset(IMapping mu){
		reset();
	}
}
