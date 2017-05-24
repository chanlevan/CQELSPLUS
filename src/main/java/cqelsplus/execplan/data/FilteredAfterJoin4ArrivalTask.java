package cqelsplus.execplan.data;

import cqelsplus.execplan.oprouters.OpRouter;

public class FilteredAfterJoin4ArrivalTask implements Runnable {
	OpRouter router;
	BatchBuff buff;
	public FilteredAfterJoin4ArrivalTask(OpRouter router, BatchBuff bb) {
		this.router = router;
		this.buff = bb;
	}
	@Override
	public void run() {
		router.routeNewArrival(buff);
	};
	
}
