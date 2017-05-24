package cqelsplus.execplan.oprouters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.ExprList;

import cqelsplus.engine.ExecContextFactory;
import cqelsplus.engine.Config;
import cqelsplus.execplan.data.BatchBuff;
import cqelsplus.execplan.data.FilteringMapping;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.data.MappingEntry;
import cqelsplus.execplan.data.POOL;

/** 
 * This class implements the router with filter operator
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see OpRouter1
 */
public class FilterExprRouter extends OpRouter1 {
	ExprList expList;
	Set<Var> varsMentioned;
	ArrayList<Integer> varIds;
	public static int count = 0;

	public FilterExprRouter(OpFilter op) {
		super(op);
		expList = ((OpFilter)op).getExprs();
		varsMentioned = expList.getVarsMentioned();
		varIds = new ArrayList<Integer>();
	}
	
	public void routeNewArrival(Object buff) {
		if (buff instanceof BatchBuff) {
			BatchBuff bb = (BatchBuff)buff;
			for  (Iterator<MappingEntry> itr = bb.iterator(); itr.hasNext();) {
				MappingEntry entry = itr.next();
				IMapping mapping = entry.getElm();
				//TODO please return it to POOL
				FilteringMapping fmu = (FilteringMapping)POOL.FilterMapping.borrowObject();
				fmu.set(mapping);
				if(!expList.isSatisfied(fmu, ExecContextFactory.current().getFilterCtx())) {
					bb.remove(entry);
					if (Config.MEMORY_REUSE) {
						entry.releaseInstance();
					}
				}
				if (Config.MEMORY_REUSE) {
					fmu.unset();
					POOL.FilterMapping.returnObject(fmu);
				}
			}
			if (!bb.isEmpty()) {
				nextRouter.routeNewArrival(bb);
			}
		} 
	}
	
	public void routeExpiration(Object buff) {
		nextRouter.routeExpiration(buff);
	}
}



 