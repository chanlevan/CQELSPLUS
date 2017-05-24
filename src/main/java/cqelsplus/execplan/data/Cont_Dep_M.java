package cqelsplus.execplan.data;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.mjoinnetwork.ProbingSequence;

public class Cont_Dep_M implements IMapping, PoolableObject {
	ProbingSequence pc;
	List<Var> inVolvedVars;
	ITuple base;
	public Cont_Dep_M() {}
	public void set(ITuple base, List<Var> vars, ProbingSequence pc) {
		this.inVolvedVars = vars;
		this.pc = pc;
		this.base = base;
	}
	
	@Override
	public List<Var> getVars() {
		// TODO Auto-generated method stub
		return this.inVolvedVars;
	}
	@Override
	public long getValue(Var var) {
		return pc.get(var, base);
	}
	@Override
	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return (Cont_Dep_M)POOL.CONT_DEP_MU.borrowObject();
	}
	@Override
	public void releaseInstance() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public ArrayList<ITuple> getLeaveTuples() {
		// TODO Auto-generated method stub
		return pc.getLeafList(base);
	}
	@Override
	public boolean contains(Cont_Dep_ExpM leaf) {
		// TODO Auto-generated method stub
		return pc.checkBelonging(leaf.getLeafTuple(), base, leaf.getVW());
	}
	@Override
	public ProbingSequence getProbingSequence() {
		// TODO Auto-generated method stub
		return pc;
	}
	@Override
	public ITuple getDataItem() {
		// TODO Auto-generated method stub
		return base;
	}
}
