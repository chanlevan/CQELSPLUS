package cqelsplus.execplan.oprouters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.aggregate.AggAvg;
import com.hp.hpl.jena.sparql.expr.aggregate.AggAvgDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCount;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVar;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMax;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMaxDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMin;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMinDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggSum;
import com.hp.hpl.jena.sparql.expr.aggregate.AggSumDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;

import cqelsplus.execplan.data.AccAverage;
import cqelsplus.execplan.data.AccCount;
import cqelsplus.execplan.data.AccMax;
import cqelsplus.execplan.data.AccMin;
import cqelsplus.execplan.data.AccSum;
import cqelsplus.execplan.data.Accumulator;
import cqelsplus.execplan.data.AggMapping;
import cqelsplus.execplan.data.BatchBuff;
import cqelsplus.execplan.data.Cont_Dep_ExpM;
import cqelsplus.execplan.data.DomAggEntry;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.data.MappingEntry;
import cqelsplus.execplan.data.POOL;
import cqelsplus.execplan.data.ToBeExpiredBuff;
import cqelsplus.execplan.mjoinnetwork.ProbingSequence;

/** 
 * This class implements the router with group-by operator
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see OpRouter1
 */
public class AggRouter extends OpRouter1 implements IStatefulRouter {

    List<ExprAggregator> aggs;
    
    ArrayList<Accumulator> accs;
	
    List<Var> groupVars;
    List<Var> accVars;
	
	HashMap<AggMapping, DomAggEntry[]> hashAll;
	
	HashMap<AggMapping, DomAggEntry[]> hashExpire;
	
	HashMap<ProbingSequence, ToBeExpiredBuff> expiringBuffMap;
	ToBeExpiredBuff expiredBuff;
	HashMap<Class, Class> classMap;
	
	HashMap<AggMapping, DomAggEntry[]> hashDispatch;
	
	public AggRouter(OpGroup opGroup) {
		super(opGroup);
		//System.out.println("group "+op);
		
		this.groupVars = opGroup.getGroupVars().getVars();
		
		this.aggs = opGroup.getAggregators();

		this.hashAll = new HashMap<AggMapping, DomAggEntry[]>();
		
		this.expiringBuffMap = new HashMap<ProbingSequence, ToBeExpiredBuff>();
		
		this.classMap = new HashMap<Class, Class>();
		
		this.accs = new ArrayList<Accumulator>();
		
		this.accVars = new ArrayList<Var>();
		
		this.expiredBuff = new ToBeExpiredBuff(this);
		init();
	}

	void init() {

		classMap.put(AggCount.class, AccCount.class);
		classMap.put(AggCountDistinct.class, AccCount.class);
		classMap.put(AggCountVar.class, AccCount.class);
		classMap.put(AggCountVarDistinct.class, AccCount.class);
		
		classMap.put(AggSum.class, AccSum.class);
		classMap.put(AggSumDistinct.class, AccSum.class);
		
		classMap.put(AggMin.class, AccMin.class);
		classMap.put(AggMinDistinct.class, AccMin.class);
		
		classMap.put(AggMax.class, AccMax.class);
		classMap.put(AggMaxDistinct.class, AccMax.class);	
		
		classMap.put(AggAvg.class, AccAverage.class);
		classMap.put(AggAvgDistinct.class, AccAverage.class);
				
		try {
			for (Iterator<ExprAggregator> itr = aggs.iterator(); itr.hasNext(); ) {
				Aggregator expr = itr.next().getAggregator();
				Accumulator acc = (Accumulator)classMap.get(expr.getClass()).newInstance();		
				acc.setAccVar(expr.getExpr().asVar());
				accs.add(acc);
				accVars.add(expr.getExpr().asVar());
			}

		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<Var> getAccVars() {
		return this.accVars;
	}
//	Var person1 = Var.alloc("person1");
//	Var person2 = Var.alloc("person2");

	@Override
	public void routeNewArrival(Object batch) {
		HashMap<AggMapping, DomAggEntry[]> toDispatch = (HashMap<AggMapping, DomAggEntry[]>)POOL.DomAggEntryMap.borrowObject();
		BatchBuff bb = (BatchBuff)batch;
		/**Now, let's consider each item arrived*/
		MappingEntry cur = bb.getFirst();
		while (cur != null) {
			IMapping elm = cur.getElm();
			/**Create an aggregate mapping*/
			AggMapping mua = (AggMapping)POOL.MUA.borrowObject();
			/**mua holds elm as a base mapping has groupVars as variable list */
			mua.set(elm, groupVars);
			/**get the accumulated value of previous arrivals if any*/
			DomAggEntry[] accEntries = hashAll.get(mua);
			/**Allocating memory if not existed*/
			if (accEntries == null) {
				accEntries = new DomAggEntry[accs.size()];
			}
			/**Go over each accumulator item*/
			for (int i = 0; i < accs.size(); i++) {
				/**assign the actual aggregate type if there is no accumulation*/
				if (accEntries[i] == null) {
					accEntries[i] = accs.get(i).aggEntry();
					accEntries[i].setAcc(accs.get(i));
					accEntries[i].reset(elm);
				} 
				/**Otherwise, update the previous accumulated with the elm*/
				else {
					accEntries[i].update(elm);
				}
				toDispatch.put(mua, accEntries);
				hashAll.put(mua, accEntries);
			}
			/**consider next element*/
			cur = cur.next;
		}
		/**and don't forget to save the considered item for expiration handling purpose in the future*/
		synchronized (expiredBuff) {
			expiredBuff.addBuff(bb);
		}
		dispatch(toDispatch, "+");
	}

	@Override
	public void routeExpiration(Object batch) {
		/**if there is no arrival item*/
		if (expiredBuff == null) {
			return;
		}
		//hashDispatch = (HashMap<AggMapping, DomAggEntry[]>)POOL.DomAggEntryMap.borrowObject();
		hashDispatch = new HashMap<AggMapping, DomAggEntry[]>();
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
		//should we implement expiration ?
		//dispatch(hashDispatch, "-");

		nextRouter.routeExpiration(batch);
	}
	
	@Override
	public void expireOne(IMapping mu) {
		AggMapping mua = (AggMapping)POOL.MUA.borrowObject();
		mua.set(mu, (ArrayList<Var>)groupVars);
		DomAggEntry[] entries = hashAll.get(mua);

		for (DomAggEntry entry : entries) {
			if(entry != null && entry.count() > 0){
				entry.expire(mua);
//				if (XSDFuncOp.compareNumeric(
//						NodeValue.makeNode(ExecContextFactory.current().decode(entry.accVal())),
//						NodeValue.makeInteger(0)) == Expr.CMP_LESS) {
//					System.out.println("Error is here");
//				}
			} else {
				entry = null;
			}
		}
		hashDispatch.put(mua, entries);
	
	}

	private  void dispatch(HashMap<AggMapping, DomAggEntry[]> toDispatch, String signal) {
		//BatchBuff batchDispatch = (BatchBuff)POOL.BatchBuff.borrowObject();
		BatchBuff batchDispatch = new BatchBuff();
		batchDispatch.setSignal(signal);
		for (Entry<AggMapping, DomAggEntry[]> entry : toDispatch.entrySet()) {
			AggMapping mua = entry.getKey();
			DomAggEntry[] values = entry.getValue();
			HashMap<Var, Long> accInfo = new HashMap<Var, Long>();
			//(HashMap<Var, Long>)POOL.accMap.borrowObject();
			for (DomAggEntry val : values) {
//				if (val.accVal() < 0) {
//					System.out.println("Negative: " + val.getAcc().accVar() + " " + val.accVal());
//				
		try {
				Var key = val.getAcc().accVar();
				//Long value = -val.accVal();
				Long value = val.accVal();
				accInfo.put(key, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			mua.setAccInfo(accInfo);
			batchDispatch.insert(mua);
		}
	
		if (!batchDispatch.isEmpty()) {
				nextRouter.routeNewArrival(batchDispatch);
		}
	}
}



