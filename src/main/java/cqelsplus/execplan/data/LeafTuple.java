package cqelsplus.execplan.data;


import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.mjoinnetwork.ProbingSequence;
import cqelsplus.execplan.utils.HashCommon;

//import it.unimi.dsi.fastutil.HashCommon;

public class LeafTuple extends TimestampedTuple {
	long[] vals;
	public LeafTuple nextNewer;
	public LeafTuple prevOlder;
	public LeafTuple(){};
    LinkedItem link;

	public void set(long[] vals){
		if (this.vals != null) {
			POOL.arrayFactory(this.vals.length).returnObject(this.vals);
		}
		this.vals = vals;
	}

	public long get(int idx) {
		if (idx >= vals.length) {
			System.out.println("Cant retrieve value with idx: " + idx);
		}
		return vals[idx];
	}

	@Override
	public int hashCode() {
		return (int)HashCommon.murmurHash3(Math.abs(timestamp));
	}
	
	@Override
	public boolean equals(Object obj) {
			if(obj instanceof LeafTuple)
				return Math.abs(timestamp)==Math.abs(((LeafTuple)obj).timestamp);
			return super.equals(obj);
	}
	
	public LinkedItem getLink() {
		return link;
	}
	
	public void setLink(LinkedItem item) {
		this.link=item;
	}
	public PoolableObject newObject() {
		return (PoolableObject) POOL.MUN.borrowObject();
	}
	public synchronized void releaseInstance() {
		this.link = null;
		//POOL.MUN.returnObject(this);
		//System.out.println(this.getClass().toString() + " pool size: " + POOL.MUN.getPoolSize());
	}

	@Override
	public long getByVar(Var var, ProbingSequence pc) {
		// TODO Auto-generated method stub
		return 0;
	}	
	
	@Override 
	public boolean isExpired(ProbingSequence pc) {
		return pc.isExpired(this);
	}
}
