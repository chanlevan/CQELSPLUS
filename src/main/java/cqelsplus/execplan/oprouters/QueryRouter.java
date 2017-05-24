package cqelsplus.execplan.oprouters;

import java.util.Iterator;

import com.hp.hpl.jena.query.Query;

import cqelsplus.engine.ContinousListener;
import cqelsplus.execplan.data.BatchBuff;
import cqelsplus.execplan.data.MappingEntry;

public class QueryRouter extends OpRouter1 {
	Query query;
	ContinousListener listener;
	QueryRouter newInstance;
	int id;
	boolean ready;
	static int idCounter = 0;
	public QueryRouter() {
		super(null);
		ready = true;
		id = idCounter++;
	}
	
	public void setQuery(Query query) {
		this.query = query;
	}
	
	public Query getQuery() {
		return query;
	}

	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	@Override
	public void routeNewArrival(Object batch) {
		String signal = ((BatchBuff)batch).getSignal();
		if (signal == null) {
			signal = "+"; 
		}
		if (signal.equals("+")) {
			update((BatchBuff)batch);
		} else {
			/***expire((BatchBuff)batch);*/
		}
	}
	
	private void update(BatchBuff buff) {
		if (!ready) return;
		//Logger.getLogger(QueryRouter.class).info("At query router id: " + id);
		Iterator<MappingEntry> itr = buff.iterator();
		while (itr.hasNext()) {
			if (listener != null) {
				listener.update(itr.next().getElm());
			} else return;
		}
	}
/***	
	private void expire(BatchBuff buff) {
		if (!ready) return;
		Iterator<MappingEntry> itr = buff.iterator();
		while (itr.hasNext()) {
			if (listener != null) {
				listener.expire(itr.next().getElm());
			} else return;
		}
	}
*/	
	@Override
	public void routeExpiration(Object batch) {
		/**Thanks. Do nothing*/
	}
	
	public void addListener(ContinousListener listener) {
		this.listener = listener;
	}
	
	public ContinousListener getListener() {
		return listener;
	}
	public void removeListener() {
		this.listener = null;
	}
	
	public void setReady(boolean val) {
		ready = val;
	}
	
	public boolean isReady() {
		return ready;
	}
	
	public void setNewInstance(QueryRouter qr) {
		this.newInstance = qr;
	}
	
	public void stopSpillOutput() {
		this.ready = false;
		if (this.newInstance != null) {
			this.newInstance.setReady(true);
		}
	}
}
