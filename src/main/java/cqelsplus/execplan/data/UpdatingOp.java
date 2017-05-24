package cqelsplus.execplan.data;

import java.util.ArrayList;

import cqelsplus.engine.Config;
import cqelsplus.execplan.oprouters.MJoinRouter;

public class UpdatingOp implements Runnable, PoolableObject {
		MJoinRouter router;
		ArrayList<ITuple> result;
		public void set(MJoinRouter router, ArrayList<ITuple> result){
			this.router = router;
			this.result = result;
		}
		
		public void execute() {
			router.routeNewArrival(result);
			if (Config.MEMORY_REUSE) {
				releaseInstance();
			}
		}
		
		public void releaseInstance() {
			POOL.UpdatingOp.returnObject(this);
		}

		@Override
		public PoolableObject newObject() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void run() {
			router.routeNewArrival(result);
			if (Config.MEMORY_REUSE) {
				releaseInstance();
			}
		}
}
