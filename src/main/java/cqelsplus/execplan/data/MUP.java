package cqelsplus.execplan.data;

//import it.unimi.dsi.fastutil.HashCommon;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.mjoinnetwork.ProbingSequence;
import cqelsplus.execplan.utils.HashCommon;

public class MUP implements IMapping, PoolableObject {
	public MUP(){}
	List<Var> pVars;
	IMapping base;

	public void set(IMapping base, List<Var> pVars) {
		this.base = base;
		this.pVars = pVars;
	}
	
	@Override
	public List<Var> getVars() {
		return this.pVars;
	}
	@Override
	public long getValue(Var var) {
		return base.getValue(var);
	}
	@Override
	public ArrayList<ITuple> getLeaveTuples() {
		return base.getLeaveTuples();
	}
	@Override
	public boolean contains(Cont_Dep_ExpM leaf) {
		return base.contains(leaf);
	}

	@Override
	public ProbingSequence getProbingSequence() {
		// TODO Auto-generated method stub
		return base.getProbingSequence();
	}

	@Override
	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseInstance() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public int hashCode() {
		int hashCode=0;
		for(int i = 0; i < pVars.size(); i ++) {
			hashCode=(int)(31*hashCode + HashCommon.murmurHash3(getValue(pVars.get(i))));
		}
		//hashCode = (int)(31*hashCode + HashCommon.murmurHash3(base.getProbingContext().hashCode()));
		return hashCode;
	}
	
	@Override
	public boolean equals(Object o) {
		return this.hashCode() == ((MUP)o).hashCode();
	}

	@Override
	public ITuple getDataItem() {
		// TODO Auto-generated method stub
		return base.getDataItem();
	}
}
