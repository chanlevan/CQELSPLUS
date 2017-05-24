package cqelsplus.execplan.mjoinnetwork;

import java.util.ArrayList;
import java.util.HashSet;

import cqelsplus.execplan.oprouters.MJoinRouter;
import cqelsplus.execplan.windows.PhysicalWindow;
import cqelsplus.execplan.windows.VirtualWindow;

public class OverlappedValue {
	int pId;
	ArrayList<Integer> nextVarPoses;
	ArrayList<Integer> prevVarPoses;
	int value;
	int weight;
	PhysicalWindow p;
	ArrayList<VirtualWindow> tmpNextVWs;
	HashSet<MJoinRouter> cQs;
	boolean considered = false;
	
	public OverlappedValue(PhysicalWindow p, ArrayList<Integer> nextVarPoses, ArrayList<Integer> prevVarPoses) {
		this.p = p;
		this.pId = p.getId();
		this.nextVarPoses = nextVarPoses;
		this.prevVarPoses = prevVarPoses;
		weight = 0;
		tmpNextVWs = new ArrayList<VirtualWindow>();
		cQs = new HashSet<MJoinRouter>();
	}	
	
	void sortIndexPos() {
		if (nextVarPoses.size() == 1)
			return;
		
		for (int i = 0; i < nextVarPoses.size() - 1; i++) {
			for (int j = i + 1; j < nextVarPoses.size(); j++) {
				if (nextVarPoses.get(i) > nextVarPoses.get(j)) {
					int tmp = nextVarPoses.get(i);
					nextVarPoses.set(i, nextVarPoses.get(j));
					nextVarPoses.set(j, tmp);
					//also swap prevarspos
					tmp = prevVarPoses.get(i);
					prevVarPoses.set(i, prevVarPoses.get(j));
					prevVarPoses.set(j, tmp);
				}
			}
		}
	}
	public int getValue() {
		return this.value;
	}
	
	public PhysicalWindow getPhysicalWindow() {
		return this.p;
	}
	
	public int getNextVarCol(int idx) {
		return this.nextVarPoses.get(idx);
	}
	
	public int getPrevVarCol(int idx) {
		return this.prevVarPoses.get(idx);
	}
	
	public ArrayList<Integer> getPrevVarCols() {
		return prevVarPoses;
	}
	
	public ArrayList<Integer> getNextVarCols() {
		return nextVarPoses;
	}
	
	public int getIdxOfProbedBuffer() {
		int result = 0;
		for (int p : nextVarPoses) {
			result = result | (1 << p);
		}
		return result;
	}
	
	
	public void addContainedMJoinRouter(MJoinRouter q) {
		cQs.add(q);
	}
	
	public int getWeight() {
		weight = cQs.size();
		return weight;
	}
	
	@Override 
	public boolean equals(Object val) {
		return this.hashCode() == ((Vertex)val).hashCode();
	}
	
	public boolean equals(int pId, ArrayList<Integer> nextVarPos, ArrayList<Integer> prevVarPos) {
		if (this.pId != pId || nextVarPoses.size() != nextVarPos.size() ||
				prevVarPoses.size() != prevVarPos.size()) 
			return false;
		for (int i = 0; i < nextVarPoses.size(); i ++) {
			if (this.nextVarPoses.get(i) != nextVarPos.get(i) || this.prevVarPoses.get(i) != prevVarPos.get(i))
				return false;
		}
		return true;
	}
	
	public void clearTmpVWsBuffer() {
		tmpNextVWs.clear();
	}
	
	public void addTmpNextVirtualWindow(VirtualWindow vw) {
		tmpNextVWs.add(vw);
	}
	
	public ArrayList<VirtualWindow> getTmpNextVirtualWindow() {
		return this.tmpNextVWs;
	}
	
	public void setConsider(boolean value) {
		considered = value;
	}
	
	public boolean isConsidered() {
		return considered;
	}
	
	public void printLog(String deep) {
		System.out.println(deep + "OV Pid: " + pId);
		System.out.print(deep + "OV next Var Pos: ");
		for (int i = 0; i < nextVarPoses.size(); i ++) {
				System.out.print(nextVarPoses.get(i) + " ");
		}
		System.out.println();		
		System.out.print(deep + "OV pre Var Pos: ");
		for (int i = 0; i < prevVarPoses.size(); i ++){
			System.out.print(prevVarPoses.get(i) + " ");
		}
		System.out.println();
		System.out.println("Shared queries num: " + cQs.size());

	}
}
 