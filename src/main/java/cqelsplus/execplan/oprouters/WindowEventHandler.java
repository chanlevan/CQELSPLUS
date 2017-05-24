
package cqelsplus.execplan.oprouters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.algebra.Op;

import cqelsplus.engine.Config;
import cqelsplus.execplan.ExecPlan;
import cqelsplus.execplan.data.BaseTuple;
import cqelsplus.execplan.data.ExpiredOpBatch;
import cqelsplus.execplan.data.ITuple;
import cqelsplus.execplan.data.InterJoinTuple;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.data.PhysicalOp;
import cqelsplus.execplan.data.Join;
import cqelsplus.execplan.data.PurgingOp;
import cqelsplus.execplan.data.UpdatingOp;
import cqelsplus.execplan.data.UpdatingOpBatch;
import cqelsplus.execplan.mjoinnetwork.JoinGraph;
import cqelsplus.execplan.mjoinnetwork.OverlappedValue;
import cqelsplus.execplan.mjoinnetwork.Vertex;
import cqelsplus.execplan.queue.EventQueue;
import cqelsplus.execplan.windows.PhysicalWindow;


public class WindowEventHandler implements PhysicalOp, OpRouter {
	ExecutorService es;
	List<PhysicalWindow> input;
	HashMap<Integer, PhysicalWindow> pIdMap;
	
	EventQueue queue;
	
	public static WindowEventHandler instance;
	
	Logger logger = Logger.getLogger(WindowEventHandler.class);
	private boolean ready = true;
	private Object monitor = null;
	
	private WindowEventHandler() {
		es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		queue = new EventQueue(es);
		queue.setExecutorService(es);
	}
	
	public static WindowEventHandler getInstance() {
		if (instance == null) {
			instance = new WindowEventHandler();
		}
		return instance;
	}
	
/**	public CoreExecutor() {
		es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		queue = new EventQueue(es);
		queue.setExecutorService(es);
	}*/
	
	public void setExecutorService(ExecutorService es) {
		this.es = es;
		queue.setExecutorService(es);
	}
	
	public ExecutorService getExecutionService() {
		return es;
	}
	
	public void setPWs(List<PhysicalWindow> pWs) {
		synchronized(this) {
	 		pIdMap = new HashMap<Integer, PhysicalWindow>();
			this.input = pWs;
			for(int i = 0;i < input.size(); i++) {
				pIdMap.put(input.get(i).getId(), input.get(i));
			}
		}
	}
	
	//MULTIPLE MJOIN propagate
	public  void probe(ITuple m, int pid, UpdatingOpBatch uop, long tick) throws InterruptedException {
//		List<JoinProbingGraph> joinGraphs = pIdMap.get(pid).getJoinProbingGraphs();
//		JoinProbingGraph jg = joinGraphs.get(joinGraphs.size() - 1);
		PhysicalWindow pw = pIdMap.get(pid);
		if (pw == null) {
			return;
		}
		JoinGraph jg = pw.getJoinGraph();
		if (jg == null) {
			return;
		}
		Vertex root = jg.getRoot(); 
		//ArrayList<IDataItem> result = (ArrayList<IDataItem>)POOL.MuList.borrowObject();
		ArrayList<ITuple> result = new ArrayList<ITuple>();
		result.add(m);
		_probe(result, root, uop, tick);
		uop.setReady(true);
		//queue.deque();			
/**			if (Settings.PRINT_LOG) {
				System.out.println("No of different queries results: " + uop.getOpList().size());
			}*/

	}
	
	private void _probe(ArrayList<ITuple> result, Vertex node, UpdatingOpBatch uop, long tick) {
		//if check if any query get this result
		for (MJoinRouter router : node.getSatisfiedQueries()) {
			//UpdatingOp our = (UpdatingOp)POOL.UpdatingOp.borrowObject();
			UpdatingOp our = new UpdatingOp();
			our.set(router, result);
			uop.add(our);
		}
		//p is root.getPW as constructed;
		List<Vertex> children = node.getChildren();
		for (int i = 0; i < children.size(); i++) {
			//get information of each child
			Vertex child = children.get(i);
			OverlappedValue ov = child.getConnectedInfo();
			//get the created index column value
			int curProbIdxCol = ov.getIdxOfProbedBuffer();
			ArrayList<Integer> prevVarCols = ov.getPrevVarCols();
			
			//Need a intermediate buffer to save the probing results
			//ArrayList<IDataItem> inter = (ArrayList<IDataItem>)POOL.MuList.borrowObject();
			ArrayList<ITuple> inter = new ArrayList<ITuple>();
			
			for (ITuple curMU : result) {
				for (Iterator<ITuple> itr = child.getPW().probe(curProbIdxCol, curMU, prevVarCols, tick); itr.hasNext();) {
					//DIJoin muj = (DIJoin)POOL.MUJ.borrowObject();
					InterJoinTuple muj = new InterJoinTuple();
				    muj.setBranches((BaseTuple)curMU, (BaseTuple)itr.next());
				    //need information to trace if this mapping expired in the future
				    muj.setVertexes(node, child);
					inter.add(muj);
				}
			}
			//if intermediate buffer has value then continue to probe
			if (!inter.isEmpty()) {
				_probe(inter, child, uop, tick);
			} 
		}
	}

	public void handleJoin(ITuple mu) throws InterruptedException {
		//UpdatingOpBatch uop = (UpdatingOpBatch)POOL.UpdatingOpBatch.borrowObject();
		UpdatingOpBatch uop = new UpdatingOpBatch();
		uop.setReady(false);
		queue.queue(uop);
		//Probing curProb = (Probing)POOL.Probing.borrowObject();
		Join join = new Join();
		join.set(this, (LeafTuple)mu, ((LeafTuple)mu).getFrom().getId(), uop, ((LeafTuple)mu).timestamp);
		if (Config.NEW_THREAD) {
			Thread t = new Thread(join);
			t.start();
		} else {
			es.execute(join);
		}
	}

	public void handleExpiration(ExpiredOpBatch eob) {
		if (!eob.getOpList().isEmpty()) {
			queue.queue(eob);
			queue.deque();
		}
	}
	
	public void handle(ITuple newLeafTuple, ExpiredOpBatch eob) throws InterruptedException {
		handleExpiration(eob);
		handleJoin(newLeafTuple);
		execute();
	}
	
	public void execute() {
		while (!queue.isFree()) {
			queue.deque();
		}
		/**es.shutdown();*/
/**		OpRun opRun = (OpRun)POOL.OpRun.borrowObject();
		opRun.set(queue);
		if (Settings.NEW_THREAD) {
			Thread t = new Thread(opRun);
			t.start();		
		} else {
			exec.execute(opRun);
	}*/
	}

	private void setReady(boolean b) {
		this.ready = b;
	}
	
	public boolean isReady() {
		return ready;
	}

	static int threadcount = 0;
	@Override
	public void deallocate() {
		es.shutdown();
		if (Config.PRINT_LOG) {
			System.out.println("Shut down execution service No: " + threadcount++);
		}
	}

	@Override
	public Op getOp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void routeNewArrival(Object object) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void routeExpiration(Object object) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void addNextRouter(OpRouter nextRouter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OpRouter getNextRouter() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setMonitor(Object monitor) {
		this.monitor = monitor;
	}
}
