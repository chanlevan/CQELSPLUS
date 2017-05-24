package cqelsplus.execplan.windows;

import java.util.ArrayList;
import java.util.List;

import cqelsplus.execplan.data.Cont_Dep_ExpM;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.data.POOL;
import cqelsplus.logicplan.algerba.OpStream;
import cqelsplus.queryparser.TripleWindow;


public class CountVirtualWindow extends VirtualWindow {
	TripleWindow trw;
	long windowSize;
	long realSize;
	
	public CountVirtualWindow(PhysicalWindow pw, OpStream op) {
		super(pw, op);
	}
	
	public CountVirtualWindow() {
		super();
	}
	
	public void init() {
		trw = (TripleWindow)(((OpStream)op).getWindow());
		windowSize = trw.getTripleNumber();
		realSize = 0;
	}
	
	public long getWindowSize() {
		return windowSize;
	}
	
	public List<Cont_Dep_ExpM> insert(LeafTuple diLeaf) {
		ArrayList<Cont_Dep_ExpM> expiredBuff = new ArrayList<Cont_Dep_ExpM>();
		//(ArrayList<Cont_Dep_ExpM>)POOL.CONT_DEP_EXPMU_LIST.borrowObject();
		newest = diLeaf;
		if (oldest == null) {
			oldest = newest;
			realSize = 1;
		} else {
			realSize++;
			if (realSize > windowSize) {
				Cont_Dep_ExpM eMU = (Cont_Dep_ExpM)POOL.CONT_DEP_EXPMU.borrowObject();
				eMU.setMUN(oldest);
				eMU.setVW(this);
				expiredBuff.add(eMU);
				oldest = oldest.nextNewer;
				realSize--;
				if (oldest == null) {
					System.out.println("Here");
				}
			} 
		}
		//pW.updateOldestInvalidItem(oldest);
		pW.updateExpTupleFromVW(oldest);
		return expiredBuff;
	}
	
	
	public boolean isExpired(LeafTuple candidate) {
		if (oldest != null) {
			return (oldest.timestamp > candidate.timestamp);
		} else {
			return false;
		}
	}
	
	/**Start reegineering*/
	public boolean isIdentical(VirtualWindow v) {
		if (!(v instanceof CountVirtualWindow)) return false;
		if (!op.equals(v.getOp())) return false;
		if (windowSize != (((CountVirtualWindow)v).getWindowSize())) return false;
		return true;
	}
	/**End reengineering*/
}
