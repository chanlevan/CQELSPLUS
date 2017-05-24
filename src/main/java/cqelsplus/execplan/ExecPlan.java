package cqelsplus.execplan;

import java.util.List;

import com.hp.hpl.jena.sparql.algebra.Op;

import cqelsplus.execplan.oprouters.PtMatchingRouter;
import cqelsplus.execplan.oprouters.QueryRouter;
/**Each query or set of queries is/are parsed into a corresponding query plan
 * Each operator is parsed into a router responsible to process and route data to next operator*/
public abstract class ExecPlan {
	protected List<Op> newQueries;
	protected List<Op> oldQueries;
	/**The query router(the highest router of execution plan tree) list*/
	protected List<QueryRouter> queryRouters;
	/**The pattern router(the lowest router of execution plan tree)*/
	protected PtMatchingRouter pmRouter;
	
	public ExecPlan() {};
	/**Build execution plan for a set of queries*/
	protected abstract void buildExecPlan();
	
	public List<QueryRouter> getQueryRouters() {
		return this.queryRouters;
	}
	
	public PtMatchingRouter getPtMatchingRouter() {
		return this.pmRouter;
	}
	
	public List<Op> getQueryList() {
		return this.newQueries;
	}
	
	public abstract boolean removeQuery(int queryId);	
}
