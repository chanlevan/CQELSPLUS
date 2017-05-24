package cqelsplus.execplan.data;


import cqelsplus.execplan.queue.EventQueue;
import cqelsplus.execplan.windows.PhysicalWindow;

public class PurgingOp implements Runnable, PoolableObject {
	EventQueue queue;
	LeafTuple firstInvalidItem;
	PhysicalWindow pW;
	public PurgingOp() {}
	public void set(PhysicalWindow pW, LeafTuple firtInvalidItem) {
		this.firstInvalidItem = firtInvalidItem;
		this.pW = pW;
	}
	
	public void execute() {
		pW.purge(firstInvalidItem);
	}

	@Override
	public PoolableObject newObject() {
		return (PoolableObject)POOL.PurgingOp.borrowObject();
	}
	@Override
	public void releaseInstance() {
		POOL.PurgingOp.returnObject(this);
	}
	@Override
	public void run() {
		pW.purge(firstInvalidItem);
	}
}
