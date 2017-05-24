package cqelsplus.execplan.oprouters;

import java.util.List;

import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.data.BatchBuff;
import cqelsplus.execplan.data.MUP;
import cqelsplus.execplan.data.MappingEntry;

public class ProjectRouter extends OpRouter1 {
	List<Var> pVars;
	public ProjectRouter(OpProject op) {
		super(op);
		pVars = op.getVars();
	}
	
	@Override
	public void routeNewArrival(Object batch) {
		BatchBuff buff = (BatchBuff)batch;
		//BatchBuff mupBuff = (BatchBuff)POOL.BatchBuff.borrowObject();
		BatchBuff mupBuff = new BatchBuff();
		mupBuff.setSignal(buff.getSignal());
		MappingEntry cur = buff.getFirst();
		while (cur != null) {
			//MUP mup = (MUP)POOL.MUP.borrowObject();
			MUP mup = new MUP();
			mup.set(cur.getElm(), pVars);
			mupBuff.insert(mup);
			cur = cur.next;
		}
		nextRouter.routeNewArrival(mupBuff);
	}
	
	public void routeExpiration(Object batch) {
		nextRouter.routeExpiration(batch);
	}
}
