package cqelsplus.execplan.data;

import java.util.ArrayList;

public class ExpiredOpBatch implements PoolableObject {
	ArrayList<ExpiringOp> ops;
	public ExpiredOpBatch() {
		this.ops = new ArrayList<ExpiringOp>();
	}

	public ArrayList<ExpiringOp> getOpList() {
		return this.ops;
	}
	
	public void add(ExpiringOp op) {
		ops.add(op);
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
}
