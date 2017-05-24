package cqelsplus.execplan.mjoinnetwork;

import java.util.ArrayList;

import cqelsplus.execplan.windows.PhysicalWindow;


public class OVManager {
	ArrayList<OverlappedValue> values;
	public OVManager() {
		values = new ArrayList<OverlappedValue>();
	}
	
	public OverlappedValue getOV(int pId, ArrayList<Integer> nextVarPos, ArrayList<Integer> prevVarPos) {
		for (OverlappedValue ov : values) {
			if (ov.equals(pId, nextVarPos, prevVarPos))
				return ov;
		}
		return null;
	}
	
	
	public OverlappedValue createOV(PhysicalWindow p, ArrayList<Integer> nextVarPos, ArrayList<Integer> prevVarPos) {
		OverlappedValue ov = new OverlappedValue(p, nextVarPos, prevVarPos); 
		values.add(ov);
		return ov;
	}
	
	public OverlappedValue getMaxOV() {
		int maxWeight = -1; 
		OverlappedValue tmp = null;
		for (OverlappedValue ov : values) {
			if ((!ov.isConsidered()) && maxWeight < ov.getWeight()) {
				maxWeight = ov.getWeight();
				tmp = ov;
			}
		}
		if (tmp != null)
			tmp.setConsider(true);
		return tmp;
	}
}
