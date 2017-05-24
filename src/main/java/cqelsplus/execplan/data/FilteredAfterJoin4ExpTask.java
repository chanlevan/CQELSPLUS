package cqelsplus.execplan.data;

import cqelsplus.execplan.oprouters.OpRouter;

public class FilteredAfterJoin4ExpTask implements Runnable {
	OpRouter router;
	Object data;
	public FilteredAfterJoin4ExpTask(OpRouter router, Object data) {
		this.router = router;
		this.data = data;
	}
	@Override
	public void run() {
		router.routeExpiration(data);
	}
	
}	
