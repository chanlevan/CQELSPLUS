package cqelsplus.execplan.data;

//import it.unimi.dsi.fastutil.HashCommon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.mjoinnetwork.ProbingSequence;
import cqelsplus.execplan.utils.HashCommon;

public class AggMapping implements IMapping, PoolableObject {
	IMapping base;
	List<Var> groupVars;
	HashMap<Var, Long> accInfo;
	public AggMapping() {}
	
	public void set(IMapping base, List<Var> groupVars) {
		this.base = base;
		this.groupVars = groupVars;
	}
	
	public void setAccInfo(HashMap<Var, Long> accInfo) {
		this.accInfo = accInfo;
	}
	
	public HashMap<Var, Long> getAccInfo() {
		return accInfo;
	}
	
	/**The hash code of an aggregate mapping is calculated by value of group variables and
	 * the hash code of the probing context to distinguish the same item from other probing context */
	@Override
	public int hashCode() {
		int hashCode=0;
		for(int i = 0; i < groupVars.size(); i ++) {
			hashCode=(int)(31*hashCode + HashCommon.murmurHash3(getValue(groupVars.get(i))));
		}
		//hashCode = (int)(31*hashCode + HashCommon.murmurHash3(base.getProbingContext().hashCode()));
		return hashCode;
	}
//	
	@Override
	public boolean equals(Object o) {
		return this.hashCode() == ((AggMapping)o).hashCode();
	}

	public long getAccVal(Var var) {
		Long val = accInfo.get(var);
		if (val == null) {
			return -1;
		}
		return val;
	}
	
	

	public List<Var> getVars() {
		return this.groupVars;
	}

	@Override
	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseInstance() {
		POOL.MUA.returnObject(this);
	}

	@Override
	public long getValue(Var var) {
		long val = base.getValue(var);
		if (val == -1) {
			val = getAccVal(var);
		}
		return val;
	}

	@Override
	public ArrayList<ITuple> getLeaveTuples() {
		// TODO Auto-generated method stub
		return base.getLeaveTuples();
	}

	@Override
	public boolean contains(Cont_Dep_ExpM leaf) {
		return base.contains(leaf);
	}

	@Override
	public ProbingSequence getProbingSequence() {
		return base.getProbingSequence();
	}

	@Override
	public ITuple getDataItem() {
		// TODO Auto-generated method stub
		return base.getDataItem();
	}
}
