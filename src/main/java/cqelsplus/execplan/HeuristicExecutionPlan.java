package cqelsplus.execplan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpN;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;

import cqelsplus.engine.Config;
import cqelsplus.execplan.mjoinnetwork.JoinGraph;
import cqelsplus.execplan.mjoinnetwork.MJoinRouterBuilder;
import cqelsplus.execplan.oprouters.WindowEventHandler;
import cqelsplus.execplan.oprouters.MJoinRouter;
import cqelsplus.execplan.oprouters.OpRouterVisitor;
import cqelsplus.execplan.oprouters.PtMatchingRouter;
import cqelsplus.execplan.oprouters.QueryRouter;
import cqelsplus.execplan.windows.PhysicalWindow;
import cqelsplus.execplan.windows.StaticWindow;
import cqelsplus.execplan.windows.VirtualWindow;
import cqelsplus.execplan.windows.WindowManager;
import cqelsplus.logicplan.algerba.MJoinTransformer;
import cqelsplus.logicplan.algerba.OpMJoin;
import cqelsplus.logicplan.algerba.OpStream;
/**One implementation for execution plan: Heuristic
 * Multiple queries are parsed into 1 execution plan based on 
 * the maximum variable-overlapped metric*/
public class HeuristicExecutionPlan extends ExecPlan {
	/**List of operators in query*/
	/**We need to store the list of query routers of previous registration turns 
	 * to trace the state of previous flow in order to stop generating output*/
	Logger logger = Logger.getLogger(HeuristicExecutionPlan.class);
	ExecPlan oldExecP;
	MJoinRouterBuilder mjoinRouterBuilder;

	public HeuristicExecutionPlan(List<Op> queryList) {
		this.newQueries = queryList;
		oldQueries = new ArrayList<Op>();
		oldExecP = null;
		mjoinRouterBuilder = null;
		buildExecPlan();
	}
	
	public HeuristicExecutionPlan(List<Op> newQueries, ExecPlan oldExecP) {
		this.newQueries = newQueries;
		this.oldExecP = oldExecP;
		this.oldQueries = oldExecP.getQueryList();
		mjoinRouterBuilder = null;
		buildMixedExecPlan();
	}

	private List<OpMJoin> searchOpMJoin(List<Op> queryOps) {
		List<OpMJoin> opMJoinList = new ArrayList<OpMJoin>();
		for (Op queryOp : queryOps) {
			List<OpMJoin> curQueryMjoinList = new ArrayList<OpMJoin>();
			//searchOpMJoin(opMJoinList, queryOp);
			searchOpMJoin(curQueryMjoinList, queryOp);
			if (!curQueryMjoinList.isEmpty()) {
				opMJoinList.addAll(curQueryMjoinList);
			} else {
				//this special case...
				buildSpecialOpMJoin(curQueryMjoinList, queryOp, null);
				//there will be at least 1 mjoin after above step for sure...
				opMJoinList.addAll(curQueryMjoinList);
			}
		}
		return opMJoinList;
	}
	
	private void buildSpecialOpMJoin(List<OpMJoin> curQueryMjoinList, Op op, Op parent) {
		if (op instanceof OpMJoin) {			
			curQueryMjoinList.add((OpMJoin)op);
		} else {
			if (op instanceof OpStream) {
				//How about op is a static op ?
				List<Op> ops = new ArrayList<Op>();
				ops.add(op);
				((OpStream)op).setSpecial();
				OpMJoin opMJoin = new OpMJoin(ops);
				curQueryMjoinList.add(opMJoin);
			}
			if (op instanceof Op1) {
				buildSpecialOpMJoin(curQueryMjoinList, ((Op1)op).getSubOp(), op);
				
			} else if (op instanceof Op2) {
				buildSpecialOpMJoin(curQueryMjoinList, ((Op2)op).getLeft(), op);
				buildSpecialOpMJoin(curQueryMjoinList, ((Op2)op).getRight(), op);
			
			} else if (op instanceof OpN) {
				for (int i = 0; i < ((OpN)op).size(); i++) {
					buildSpecialOpMJoin(curQueryMjoinList, ((OpN)op).get(i), op);
				}
			}
		}
	}

	private void searchOpMJoin(List<OpMJoin> curQueryMjoinList, Op op) {
		if (op instanceof OpMJoin) {
			curQueryMjoinList.add((OpMJoin)op);
			//op2IdMap.put((OpMJoin)op, opMJoinList.size() - 1);
			
			for (int i = 0; i < ((OpMJoin)op).size(); i++) {
				searchOpMJoin(curQueryMjoinList, ((OpN)op).get(i));
			}
		} else {
			if (op instanceof Op1) {
				searchOpMJoin(curQueryMjoinList, ((Op1)op).getSubOp());
				
			} else if (op instanceof Op2) {
				searchOpMJoin(curQueryMjoinList, ((Op2)op).getLeft());
				searchOpMJoin(curQueryMjoinList, ((Op2)op).getRight());
			
			} else if (op instanceof OpN) {
				for (int i = 0; i < ((OpN)op).size(); i++) {
					searchOpMJoin(curQueryMjoinList, ((OpN)op).get(i));
				}
			}
		}
	}
	
	List<Op> transform2MJoinBasedQueries(List<Op> queries) {
		MJoinTransformer mJoinTransformer = new MJoinTransformer();
		List<Op> transformedQOps = new ArrayList<Op>();
		for (int i = 0; i < queries.size(); i++) {
			transformedQOps.add(mJoinTransformer.transform(queries.get(i)));
		}
		return transformedQOps;
	}
	
	
	protected void buildExecPlan() {
		if (Config.PRINT_LOG) {
			logger.info("REGISTERING QUERIES");
		}
		/**Build join execution plan first*/
		List<Op> mjoinBasedQueries = transform2MJoinBasedQueries(newQueries);
		List<OpMJoin> emptyOldOps = new ArrayList<OpMJoin>();
		List<OpMJoin> opMJoinList = searchOpMJoin(mjoinBasedQueries);
		
		mjoinRouterBuilder = new MJoinRouterBuilder(emptyOldOps, opMJoinList);
		/**Build execution plan for all queries */		
		installQueriesExecutionPlan(mjoinBasedQueries);
	}
	
	/**This method is dealing with the execution plan for both old and new queries*/
	void buildMixedExecPlan() {
		if (Config.PRINT_LOG) {
			logger.info("REGISTERING QUERIES(OLD QUERIES ARE ALSO CONSIDERED)");
		}
		/**Build join execution plan first*/
		List<Op> oldMJoinBasedQueries = transform2MJoinBasedQueries(oldQueries);
		List<Op> newMJoinBasedQueries = transform2MJoinBasedQueries(newQueries);
		
		List<OpMJoin> oldOpMJoinList = searchOpMJoin(oldMJoinBasedQueries);
		List<OpMJoin> newOpMJoinList = searchOpMJoin(newMJoinBasedQueries);
		
		mjoinRouterBuilder = new MJoinRouterBuilder(oldOpMJoinList, newOpMJoinList);
		/**Build execution plan for all queries */		
		List<Op> allTransformedOps = oldMJoinBasedQueries;
		allTransformedOps.addAll(newMJoinBasedQueries);
		installQueriesExecutionPlan(allTransformedOps);		
	}
	
	/**
	 * Chain physical components together
	 */
	void installQueriesExecutionPlan(List<Op> transformedQOps) {
		/**Build routers for other operators*/
    	HashMap<OpMJoin, Integer> op2IdMap = new HashMap<OpMJoin, Integer>();
    	List<OpMJoin> opMJoinList = mjoinRouterBuilder.getOpMJoinList();   	
		for (int i = 0; i < opMJoinList.size(); i++) {
			op2IdMap.put(opMJoinList.get(i), i);
		}
		queryRouters = new ArrayList<QueryRouter>();
		for (Op queryOp : transformedQOps) {
			OpRouterVisitor mjv = new OpRouterVisitor(mjoinRouterBuilder.getAllMJoinRouters(), op2IdMap);
			queryOp.visit(mjv);
			queryRouters.add(mjv.getQueryRouter());
		}    	
		/**Add listener for re-planned queries*/
		if (oldExecP != null) {
			List<QueryRouter> oldQueryRouters = oldExecP.getQueryRouters();
			if (oldQueryRouters != null) {
				for (int i = 0; i < oldQueryRouters.size(); i++) {
					QueryRouter oldQR = oldQueryRouters.get(i);
					/**Set the query id of re-planned query routers to its old id*/
					queryRouters.get(i).setId(oldQR.getId());
					/**Add listener of old queries to re-planned queries*/
					queryRouters.get(i).addListener(oldQR.getListener());
				}
			}
		}
		
		/**create the pattern matching router for this (set of) query(ies)*/

	   	List<PhysicalWindow> pWs = WindowManager.getInstance().getPWs();


	   	JoinGraph[] jgs = new JoinGraph[pWs.size()];
	   	for (int i = 0; i < pWs.size(); i++) {
	   		PhysicalWindow pW = pWs.get(i);
	   		/**There is no need to add Routing graph for Static Window*/
	   		if (pW instanceof StaticWindow) continue;
	   		jgs[i] = new JoinGraph(mjoinRouterBuilder.getAllMJoinRouters(), pW);
	   	}
	   	//logger.info("finished add join graph");
	   	
	   	WindowEventHandler weh =  WindowEventHandler.getInstance();
	   	PtMatchingRouter ptmr = PtMatchingRouter.getInstance();
	   	
	   	Lock lock = ptmr.getLock();
	   	lock.lock();
	   	/**Start critical section*/
	   	weh.setPWs(pWs);

	   	for (int i = 0; i < pWs.size(); i++) {
	   		PhysicalWindow pW = pWs.get(i);
	   		/**There is no need to add Routing graph for Static Window*/
	   		if (pW instanceof StaticWindow) continue;
	   		pW.addJoinProbingGraph(jgs[i]);
	   	}

	   	
	   	/**Load static data and bind stream input*/
    	for (PhysicalWindow pW : pWs) {
			if (pW instanceof StaticWindow) {
				((StaticWindow) pW).loadData((OpQuadPattern)pW.getVWs().get(0).getOp());
			}
    	}
    	
	   	//logger.info("finished load statics");
    	/**Finally, bind stream input*/
    	for (PhysicalWindow pW : pWs) {
    		if (!(pW instanceof StaticWindow)) {
    			ptmr.addObserver(pW, ((OpStream)pW.getVWs().get(0).getOp()).getQuad());
    		}
    	}
	   	//logger.info("finished add observer");
		for (MJoinRouter mj : mjoinRouterBuilder.getAllMJoinRouters()) {
			mj.SetSuspend(false);
		}
		/**End critical section*/
		lock.unlock();
		
	   	if (Config.PRINT_LOG) {
	   		WindowManager.getInstance().printLog();
	   	}
	}
	
	public List<Op> getQueryList() {
		List<Op> allQueries = new ArrayList<Op>(oldQueries);
		allQueries.addAll(newQueries);
		return allQueries;
	}
	

	@Override
	public boolean removeQuery(int queryId) {
		int foundId = -1;
		for (int i = 0; i < queryRouters.size(); i++) {
			if (queryRouters.get(i).getId() == queryId) {
				foundId = i;
				break;
			}
		}
		if (foundId == -1) return false;
		
		MJoinRouter mJoinRouter = mjoinRouterBuilder.getMJoinRouterAt(foundId);
		mJoinRouter.SetSuspend(true);
		
		WindowManager wm = WindowManager.getInstance();
		for (VirtualWindow vW : mJoinRouter.getVWs()) {
			PhysicalWindow pW = vW.getPW();
			vW.killme();
			pW.removeVW(vW);
			wm.removeVirtualWindow(vW);
			if (pW.getVWs().isEmpty()) {
				//wm.removePhysicalWindow(pW);
				//TODO
			}
		}
		logger.info("Query Id: " + queryId + "has been removed");
		return true;
	}
}
