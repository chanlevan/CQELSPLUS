package cqelsplus.execplan.data;


import cqelsplus.engine.Config;
import cqelsplus.execplan.oprouters.WindowEventHandler;
import cqelsplus.execplan.windows.AWB;

public class Join implements Runnable,PoolableObject {
	
	WindowEventHandler weh;
	int pWId;
	long tick;
	ITuple m;
	AWB output;
	UpdatingOpBatch uop;
	public Join(){};

	public void set(WindowEventHandler ce, ITuple m, int pWId, UpdatingOpBatch uop, long tick){
		this.weh = ce;
		this.pWId = pWId;
		this.tick = tick;
		this.m = m;
		this.uop = uop;
	}
	
	public void run() {
		try {
			weh.probe(m, pWId, uop, tick);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (Config.MEMORY_REUSE) {
			releaseInstance();
		}
	}
	
	public PoolableObject newObject() {
		return (PoolableObject)POOL.Probing.borrowObject();
	}
	public void releaseInstance() {
		POOL.Probing.returnObject(this);
	}
}