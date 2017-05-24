package cqelsplus.execplan.data;

import java.util.ArrayList;

public class UpdatingOpBatch {
	ArrayList<UpdatingOp> ops;
	boolean ready;
	public UpdatingOpBatch() {
		ready = false;
		this.ops = new ArrayList<UpdatingOp>();
	}

	public boolean isReady() {
		return ready;
	}
	
	public void setReady(boolean val) {
		this.ready = val;
	}
	
	public ArrayList<UpdatingOp> getOpList() {
		return this.ops;
	}
	
	public void add(UpdatingOp op) {
		ops.add(op);
	}
}
