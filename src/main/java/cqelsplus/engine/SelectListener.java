package cqelsplus.engine;


import cqelsplus.execplan.oprouters.QueryRouter;

public abstract class SelectListener implements ContinousListener {
	protected QueryRouter qr;
	public void setQueryRouter(QueryRouter qr) {
		this.qr = qr;
	}
}
