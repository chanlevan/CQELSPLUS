package cqelsplus.execplan.oprouters;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.engine.Config;
import cqelsplus.execplan.data.AggMapping;
import cqelsplus.execplan.data.BatchBuff;
import cqelsplus.execplan.data.MappingEntry;
import cqelsplus.execplan.data.POOL;


public class ExtendRouter extends OpRouter1 {
	List<Var> extendedVars;
	HashMap<Var, Var> accMap;
	public ExtendRouter(OpExtend op) {
		super(op);
		extendedVars = op.getVarExprList().getVars();
		accMap = new HashMap<Var, Var>();
	}
	
	public void addSubRouter(OpRouter subRouter) {
		super.addSubRouter(subRouter);
		//can we confirm before Extend is Agg ?
		List<Var> subAggVars = ((AggRouter)subRouter).getAccVars();
		for (int i = 0; i < subAggVars.size(); i++) {
			accMap.put(subAggVars.get(i), extendedVars.get(i));
		}
	}

	@Override
	public void routeNewArrival(Object batch) {
		BatchBuff buff = (BatchBuff)batch;
		
		for (Iterator<MappingEntry> itr = buff.iterator(); itr.hasNext(); ) {
			HashMap<Var, Long> newAccInfo = (HashMap<Var, Long>)POOL.accMap.borrowObject();
			MappingEntry entry = itr.next();
			AggMapping mua = (AggMapping)entry.getElm();
			HashMap<Var, Long> accInfo = mua.getAccInfo();
			for (Var var : accInfo.keySet()) {
				Var extendedVar = accMap.get(var);
				newAccInfo.put(extendedVar, accInfo.get(var));
			}
			mua.setAccInfo(newAccInfo);
			if (Config.MEMORY_REUSE) {
				POOL.accMap.returnObject(accInfo);
			}
		}
		nextRouter.routeNewArrival(buff);
	}

	@Override
	public void routeExpiration(Object batch) {
		nextRouter.routeExpiration(batch);	
	}

}
