package cqelsplus.execplan.data;

import cqelsplus.execplan.windows.VirtualWindow;

public class Cont_Dep_ExpM implements PoolableObject {
	LeafTuple base;
	VirtualWindow vw;
	public Cont_Dep_ExpM() {}
	public void set(LeafTuple base, VirtualWindow vw) {
		this.base = base;
		this.vw = vw;
	}
	
	public void setMUN(LeafTuple base) {
		this.base = base;
	}
	
	public void setVW(VirtualWindow vw) {
		this.vw = vw;
	}

	public VirtualWindow getVW() {
		return this.vw;
	}

	public LeafTuple getLeafTuple() {
		return this.base;
	}
	
	@Override
	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return (Cont_Dep_ExpM)POOL.CONT_DEP_MU.borrowObject();
	}
	@Override
	public void releaseInstance() {
		// TODO Auto-generated method stub
		
	}
}
