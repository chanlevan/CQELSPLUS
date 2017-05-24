package cqelsplus.execplan.data;

import java.util.List;

import cqelsplus.execplan.oprouters.MJoinRouter;
import cqelsplus.execplan.windows.VirtualWindow;

public class ExpiringOp implements Runnable, PoolableObject {
	List<Cont_Dep_ExpM> buff;
	MJoinRouter mjoinRouter;
	VirtualWindow vWin;
	
	public ExpiringOp() {}

	public void set(List<Cont_Dep_ExpM> buff, MJoinRouter mjoinRouter, VirtualWindow vWin) {
		this.buff = buff;
		this.mjoinRouter = mjoinRouter; 
		this.vWin = vWin;
	}
	
	public void execute() {
		mjoinRouter.routeExpiration(buff);
		//releaseInstance();
	}

	@Override
	public PoolableObject newObject() {
		return (PoolableObject)POOL.ExpiringOp.borrowObject();
	}
	@Override
	public void releaseInstance() {
		POOL.ExpiringOp.returnObject(this);
	}

	@Override
	public void run() {
		try {
			mjoinRouter.routeExpiration(buff);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
