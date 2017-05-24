package cqelsplus.execplan.queue;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import cqelsplus.execplan.data.ExpiredOpBatch;
import cqelsplus.execplan.data.ExpiringOp;
import cqelsplus.execplan.data.PurgingOp;
import cqelsplus.execplan.data.UpdatingOp;
import cqelsplus.execplan.data.UpdatingOpBatch;
import cqelsplus.execplan.oprouters.OpRouter;

public class EventQueue {
	protected Queue<Object> queue;
	ArrayList<OpRouter> listerners;	
	public static Object monitorProcessing = new Object();
	public static Boolean isBeingWaited = false;
	boolean dequeing=false;
	final static Logger logger = Logger.getLogger(EventQueue.class);
	ExecutorService es;
	public EventQueue(ExecutorService es) {
		listerners=new ArrayList<OpRouter>();
		queue = new LinkedBlockingDeque<Object>();
	}
	
	public void setExecutorService(ExecutorService es) {
		this.es = es;
	}
	
	public void queue(Object buff){
		queue.add(buff);
	}
	public void regListener(OpRouter router){
		listerners.add(router);
	}
	
	public boolean isFree() {
		return (queue.isEmpty());
	}
	
	public void deque() {
		synchronized (queue) {
			Object task = queue.peek();
			while (task != null) {
				if (task instanceof UpdatingOpBatch) {
					if (!((UpdatingOpBatch)task).isReady()) {
						return;
					} else {
						update((UpdatingOpBatch)task);
						queue.poll();
					}
				} else if (task instanceof ExpiredOpBatch) {
					invalidate((ExpiredOpBatch)task);
					queue.poll();
				} 
				else if (task instanceof PurgingOp) {
					purgeBuffer((PurgingOp)task);
					queue.poll();
				}
				task = queue.peek();
			}
		}
	} 
	

	public void update(UpdatingOpBatch uop) {
		for (UpdatingOp op : uop.getOpList()) {
			op.execute();
			//es.execute(op);
		}
	}

	public void invalidate(ExpiredOpBatch eop) {
		for (ExpiringOp op : eop.getOpList()) {
			op.execute();
			//es.execute(op);
		}
	}
	
	public void purgeBuffer(PurgingOp op) {
		//es.execute(op);
		op.execute();
	}

	public int size() {
		// TODO Auto-generated method stub
		return queue.size();
	}
}
