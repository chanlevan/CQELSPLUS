package cqelsplus.execplan.oprouters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;

import cqelsplus.execplan.data.BatchBuff;
import cqelsplus.execplan.data.Cont_Dep_ExpM;
import cqelsplus.execplan.data.DomEntry;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.data.MappingEntry;
import cqelsplus.execplan.data.POOL;
import cqelsplus.execplan.data.ToBeExpiredBuff;

public class DistinctRouter extends OpRouter1 implements IStatefulRouter {
	HashMap<IMapping, DomEntry> globalHash;
	HashMap<IMapping, DomEntry> hashDispatch;
	ToBeExpiredBuff expiredBuff;
	public DistinctRouter(OpDistinct op) {
		super(op);
		globalHash= (HashMap<IMapping, DomEntry>)POOL.DomMappingEntryMap.borrowObject();
		expiredBuff = new ToBeExpiredBuff(this);
	}

	@Override
	public void routeNewArrival(Object batch)  {
		HashMap<IMapping, DomEntry> toDispatch=new HashMap<IMapping, DomEntry>();
		BatchBuff buff = (BatchBuff)batch;
		MappingEntry cur = buff.getFirst();
		while(cur != null) {
			/**Do 2 things:
			 * 1. update new distinct value
			 * 2. Save the count of each leaf for handling expiration*/
			/**Now is the step 1*/
			IMapping curMU = cur.getElm();
			DomEntry entry=globalHash.get(curMU);
			if(entry == null){
				entry=(DomEntry)POOL.DomEntry.borrowObject();
				entry.setCount(1);
				toDispatch.put(curMU, entry);
			}
			else entry.incCount();

			globalHash.put(curMU, entry);
			/**go next*/
			cur = cur.next;
		}
		synchronized (expiredBuff) {
			expiredBuff.addBuff(buff);
		}
		dispatch(toDispatch, "+");
	}
	
	private void dispatch(HashMap<IMapping, DomEntry> toDispatch, String signal) {
		BatchBuff batchDispatch = (BatchBuff)POOL.BatchBuff.borrowObject();
		batchDispatch.setSignal(signal);
		for (Entry<IMapping, DomEntry> entry : toDispatch.entrySet()) {
			IMapping mapping = entry.getKey();
			batchDispatch.insert(mapping);
		}
	
		if (!batchDispatch.isEmpty()) {
				nextRouter.routeNewArrival(batchDispatch);
		}
	}

	@Override
	public void routeExpiration(Object batch) {
		/**if there is no arrival item*/
		//hashDispatch = (HashMap<IMapping, DomEntry>)POOL.DomMappingEntryMap.borrowObject();
		hashDispatch = new HashMap<IMapping, DomEntry>();
		/**definitely the coming expiration batch are all expired MU leaf*/
		ArrayList<Cont_Dep_ExpM> buff = (ArrayList<Cont_Dep_ExpM>)batch;
		/**Go through the buffer and deal with each expired leaft mapping*/
		for (Cont_Dep_ExpM expMU : buff) {
			try {
				/**Consider each item in saved expired buffer if they are related to this leaf*/
				synchronized(expiredBuff) {
					expiredBuff.purge(expMU);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		dispatch(hashDispatch, "-");
		nextRouter.routeExpiration(batch);
		
	}

	@Override
	public void expireOne(IMapping mu) {
		// TODO Auto-generated method stub
		DomEntry entry = globalHash.get(mu);
		if(entry != null && entry.count() > 0){
			entry.decrCount();
		} else {
			entry = null;
			hashDispatch.put(mu, entry);			
		}

	}

}
