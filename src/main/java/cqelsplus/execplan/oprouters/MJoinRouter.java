package cqelsplus.execplan.oprouters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.engine.Config;
import cqelsplus.execplan.data.BatchBuff;
import cqelsplus.execplan.data.Cont_Dep_M;
import cqelsplus.execplan.data.FilteredAfterJoin4ArrivalTask;
import cqelsplus.execplan.data.FilteredAfterJoin4ExpTask;
import cqelsplus.execplan.data.ITuple;
import cqelsplus.execplan.data.InterJoinTuple;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.mjoinnetwork.ProbingSequence;
import cqelsplus.execplan.windows.VirtualWindow;

public class MJoinRouter extends OpRouterN {
	static int num = 0;
	boolean covered = false;
	List<VirtualWindow> vWs;
	OpRouterBase opFlow;
	ExecutorService es;
	/**
	 * Each MJoin router contains a list of routing paths
	 * coming from different physical windows, right ?
	 * The key of the map is the id of physical window
	 * The value is the path which started from the virtual window 
	 * corresponding to that physical window
	 */
	Map<Integer, ProbingSequence> id2probingSequencesMap;
	Map<Integer, List<VirtualWindow>> pW2VWMap;
	LinkedBlockingDeque<BatchBuff> newArrivalQueue;
	Set<VirtualWindow> startedVW;
	boolean arrival_routing;
	boolean expiration_routing;
	boolean suspended = false;
	boolean havingSharedVirtualWindows = false;
	
	public MJoinRouter(Op op) {
		super(op);//TODO
		id = count ++;
		newArrivalQueue = new LinkedBlockingDeque<BatchBuff>();
		id2probingSequencesMap = new HashMap<Integer, ProbingSequence>();
		startedVW = new HashSet<VirtualWindow>();
		es = WindowEventHandler.getInstance().getExecutionService();
	}
	
	public void setVirtualWindows(List<VirtualWindow> vWs) {
		this.vWs = vWs;
		checkSharedVirtualWindow();
	}
	
	private void checkSharedVirtualWindow() {
		Map<Integer, List<VirtualWindow>> map = new HashMap<Integer, List<VirtualWindow>>();
		for (VirtualWindow vW : this.vWs) {
			List<VirtualWindow> vWList = map.get(vW.getPW().getId());
			if (vWList == null) {
				vWList = new ArrayList<VirtualWindow>();
			}
			vWList.add(vW);
			map.put(vW.getPW().getId(), vWList);
		}
		pW2VWMap = map;
		for (Entry<Integer, List<VirtualWindow>> entry : map.entrySet()) {
			if (entry.getValue().size() > 1) {
				havingSharedVirtualWindows = true;
			}
		}
	}
	
	public boolean havingSharedVirtualWindows() {
		return havingSharedVirtualWindows;
	}
	

	public boolean checkStartedVW(VirtualWindow startVW) {
		return this.startedVW.contains(startVW);
	}

	public int getId() {
		return id;
	}
	/**
	 * When q query is covered, every virtual window is set visited
	 */
	public void setCovered(boolean value) {
		covered = value;
		if (covered) {
			for (VirtualWindow v : vWs) {
				v.setVisited(true);
			}
		}
	}

	public boolean isCovered() {
		return covered;
	}
	
	public List<VirtualWindow> getVWs() {
		return vWs;
	}
	
	@Override
	public int hashCode() {
		return this.id;
	}
	
	@Override
    public boolean equals(Object o) {
		return this.id == ((MJoinRouter)o).id;
	}
	
	public static int count = 0;

	@Override
	public void routeNewArrival(Object batch) {
		//Logger.getLogger(MJoinRouter.class).info("at routNewArrival of MJoinRouter with id " + id);
		if (suspended) return;
		//1. Eliminate redundant results
		//1 batch comes from the the triggering of the same virtual window
		ArrayList<ITuple> output = (ArrayList<ITuple>)batch;
		
		if (!output.isEmpty()) {
			//BatchBuff bb = (BatchBuff)POOL.BatchBuff.borrowObject();
			BatchBuff bb = new BatchBuff();
			/**Start re-engineering*/
			ProbingSequence ps = null;
			int psId = -1;
			if (output.get(0) instanceof InterJoinTuple) {
				psId = ((InterJoinTuple)output.get(0)).getRightVertex().getId();
				ps = id2probingSequencesMap.get(psId);
				
			} else if (output.get(0) instanceof LeafTuple) {
				/**Special case with 1 virtual window. Any other ?*/
				ps = id2probingSequencesMap.values().iterator().next();
			}
			
			/**En re-engineering*/
			if (ps != null) {
				for (ITuple mu : output) {
					if (!mu.isExpired(ps)) {
						//Cont_Dep_M cdmu = (Cont_Dep_M)POOL.CONT_DEP_MU.borrowObject();
						Cont_Dep_M cdmu = new Cont_Dep_M();
						cdmu.set(mu, ps.getVars(), ps);
						bb.insert(cdmu);
					}
				}
			}
			else {
				throw new NoSuchElementException("We don't know where this batch come from, sorry !");
			}
			//2. Route the proper result to the next operator
			if (!bb.isEmpty()) {
			try {
				es.execute(new FilteredAfterJoin4ArrivalTask(nextRouter, bb));
				//nextRouter.routeNewArrival(bb);
			} catch (Exception e) {
					e.printStackTrace();
			}
			} 
			//after finished routing, then release instance, right ?
			if (Config.MEMORY_REUSE) {
				bb.releaseInstance();
			}
		} 
	}
	
	public void routeExpiration(Object buff) {
		if (suspended) return;
		es.execute(new FilteredAfterJoin4ExpTask(nextRouter, buff));
		//nextRouter.routeExpiration(buff);
	}

	public void saveProbingSequence(VirtualWindow vw, List<Var> allVars, int psId) {
		ArrayList<VirtualWindow> path = new ArrayList<VirtualWindow>();
		do {
			path.add(0, vw);
			vw = vw.getBefore();
		}
		while (vw != null);
		startedVW.add(path.get(0));
		
		ProbingSequence pc = new ProbingSequence(path, allVars);
		id2probingSequencesMap.put(psId, pc);
		
		if (Config.PRINT_LOG) {
			System.out.print("Routing path of query " + this.id + 
					" from the physical window id: " + path.get(0).getPW().getId()+ ":: ");
			for (int i = 0; i < path.size(); i++) {
				System.out.print("v"+path.get(i).getId() + " ");
			}
			System.out.println(" probing sequence id: " + psId);
		}
	}
	
	public void saveProbingSequence(List<VirtualWindow> ps, List<Var> allVars, int psId) {
		List<VirtualWindow> path = new ArrayList<VirtualWindow>();
		for (int i = 0; i < ps.size(); i++) {
			path.add(ps.get(i));
		}
		startedVW.add(path.get(0));
		
		ProbingSequence pc = new ProbingSequence(path, allVars);
		id2probingSequencesMap.put(psId, pc);
		
		if (Config.PRINT_LOG) {
			System.out.print("Routing path of query " + this.id + 
					" from the physical window id: " + path.get(0).getPW().getId()+ ":: ");
			for (int i = 0; i < path.size(); i++) {
				System.out.print("v"+path.get(i).getId() + " ");
			}
			System.out.println(" probing sequence id: " + psId);
		}
	}
	
	

	/**Start reengineering*/
	public void SetSuspend(boolean val) {
		suspended = val;
	}
	/**End reenineering*/

}
