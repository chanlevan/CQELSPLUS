package cqelsplus.engine;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.Quad;

import cqelsplus.execplan.ExecPlan;
import cqelsplus.execplan.HeuristicExecutionPlan;
import cqelsplus.execplan.utils.MJoinUtils;
import cqelsplus.execplan.utils.OpUtils;
import cqelsplus.execplan.windows.PhysicalWindow;
import cqelsplus.execplan.windows.WindowManager;
import cqelsplus.logicplan.algerba.MJoinTransformer;
import cqelsplus.logicplan.algerba.OpMJoin;
import cqelsplus.logicplan.algerba.OpStream;

public class QueryMigrator {
	List<ExecPlan> execPlans;
	private static QueryMigrator queryMigrator = null;
	static Logger logger = Logger.getLogger(QueryMigrator.class);
	private QueryMigrator() {
		execPlans = new ArrayList<ExecPlan>();
	}
	
	public static QueryMigrator getInstance() {
		if (queryMigrator == null) {
			queryMigrator = new QueryMigrator();
		} 
		return queryMigrator;
	}
	
	public ExecPlan buildExecutionPlan(List<Op> queries) {
		ExecPlan oldEp = null;
		if (!execPlans.isEmpty()) {
			oldEp = execPlans.get(execPlans.size() - 1);
		}
		if (oldEp != null) {
			ExecPlan ep = new HeuristicExecutionPlan(queries, oldEp);	
			execPlans.add(ep);
			return ep;
		} else {
			ExecPlan ep = new HeuristicExecutionPlan(queries);
			execPlans.add(ep);
			return ep;
		}
	}
	
	public ExecPlan buildExecutionPlan(Op query) {
		List<Op> queries = new ArrayList<Op>();
		queries.add(query);
		return buildExecutionPlan(queries);
	}
	
	public boolean removeQuery(int queryId) {
		if (execPlans.isEmpty()) {
			logger.error("No query to remove !");
			return false;
		} else {
			ExecPlan ep = execPlans.get(execPlans.size() - 1);
			return ep.removeQuery(queryId);
		}

	}

	public int getCreatedBufferNum(Op query) {
		int numOfBuffers = 0;
		WindowManager wm = WindowManager.getInstance();
		numOfBuffers = wm.getPWs().size();
		
		MJoinTransformer mJoinTransformer = new MJoinTransformer();
		Op mjoinOpsQuery = mJoinTransformer.transform(query);
		List<Op> ops = new ArrayList<Op>();
		ops.add(mjoinOpsQuery);
		List<OpMJoin> mjoinList = MJoinUtils.searchOpMJoin(ops);
		
		for (OpMJoin op : mjoinList) {
			for (Op subOp : op.getElements()) {
				Quad quad = null;
				if (subOp.getClass().equals(OpStream.class)) {
					quad = ((OpStream)subOp).getQuad();
				} else if (subOp.getClass().equals(OpQuadPattern.class)) {
					quad = ((OpQuadPattern)subOp).getPattern().get(0);
				}
				if (quad != null) {
					PhysicalWindow pw = wm.getNotUpatedPhysicalWindow(OpUtils.ptCode(quad));
					if (pw == null) {
						numOfBuffers++;
					}
				}
			}
		}
		return numOfBuffers;
	}
	
}
