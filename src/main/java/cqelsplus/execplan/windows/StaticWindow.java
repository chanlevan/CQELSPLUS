package cqelsplus.execplan.windows;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

import cqelsplus.engine.ExecContext;
import cqelsplus.engine.ExecContextFactory;
import cqelsplus.execplan.data.DomEntry;
import cqelsplus.execplan.data.ITuple;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.data.NLinks;
import cqelsplus.execplan.data.POOL;
import cqelsplus.execplan.data.iterator.NullIterator;
import cqelsplus.execplan.data.iterator.StaticRingIterator;
import cqelsplus.execplan.indexes.DomainIndex;
import cqelsplus.execplan.mjoinnetwork.JoinGraph;
import cqelsplus.execplan.utils.OpUtils;


public class StaticWindow extends PhysicalWindow {
	ArrayList<long[]> indexedData = new ArrayList<long[]>();
	ArrayList<BigInteger> accordBI = new ArrayList<BigInteger>();
	ExecContext context;
	boolean cached = false;
	List<long[]> enQuadList = new ArrayList<long[]>();
	List<Var> vars = null;
	
	public StaticWindow(String pWCode, int id) {
		super(pWCode, id);
		context = ExecContextFactory.current();
	}
	
	private void init(OpQuadPattern op) {
		vars = OpUtils.parseVarsToArray(op);
		QueryIterator itr = context.loadGraphPattern(op, context.getDataset());
		
		while (itr.hasNext()) {
			Binding binding = itr.next();
			long[] vals = new long[varColumns.length];
			for(int i = 0; i < varColumns.length; i ++) {
				vals[i] = context.engine().encode(binding.get(vars.get(i)));
			}
			enQuadList.add(vals);
		}	
	}
	
	/**pre-load static data for windows which are not stream*/
	public void loadData(OpQuadPattern op) {
		if(!cached) {
			init(op);
			cached = true;
		}
		
		synchronized (this) {
			dataBuffer = new ArrayDeque<LeafTuple>();
			for (DomainIndex di : indexes) {
				di.release();
			}
		}	
		
		for (long[] vals : enQuadList) {
			LeafTuple mun = new LeafTuple();
			mun.timestamp = -1;
			mun.set(vals);
			this.insert(mun);
		}
		cached = true;
	}
	
	@SuppressWarnings("unchecked")
	public void insert(ITuple diLeaf) {
		dataBuffer.add((LeafTuple)diLeaf);
		NLinks mappingLink = new NLinks(indexes.size());
		diLeaf.setLink(mappingLink);
		for(int i=0;i<indexes.size();i++) {
			BitSet b = bitIndexedColumns.get(i);
			if (b.cardinality() == 1) {
				long key = diLeaf.get(b.nextSetBit(0));
				DomainIndex t = indexes.get(i);
				t.add(i, key, diLeaf);
			} else {
				BigInteger key = BigInteger.valueOf(0);
				int tmp = 0;
				for (int j = b.nextSetBit(0); j >= 0; j = b.nextSetBit(j+1)) {
					BigInteger bi = BigInteger.valueOf(diLeaf.get(j));
					key = key.shiftLeft(64 * tmp).or(bi); 
					tmp++;
				}
				DomainIndex t = indexes.get(i);
				t.add(i, key, diLeaf);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public Iterator<ITuple> probe(int indexVal, ITuple mu, ArrayList<Integer> prevVarsPos, long tick) {
		DomEntry entry = null;
		if (prevVarsPos.size() == 1) {
			int varPos = prevVarsPos.get(0);
			long key = mu.get(varPos);
			entry = indexes.get(columnsCode2ArrIdMap[indexVal]).get(key);

		} else {
			BigInteger key = BigInteger.valueOf(0);
			for (int j = 0; j < prevVarsPos.size(); j++) {
				long longKey = mu.get(prevVarsPos.get(j));
				BigInteger bi = BigInteger.valueOf(longKey);
				key = key.shiftLeft(64 * j).or(bi);
			}
			entry = indexes.get(columnsCode2ArrIdMap[indexVal]).get(key);
		}

		if (entry != null) {
			return new StaticRingIterator((LeafTuple)entry.getLink(), columnsCode2ArrIdMap[indexVal], entry.count()); 
		}
		return NullIterator.instance();
	}
	
	public void addJoinProbingGraph(JoinGraph routingGraph) {}
	
	public void clearCache() {
		dataBuffer.clear();
	}
	/**Start re-engineering*/
	public boolean cached() {
		return cached;
	}
	/**End re-engineering*/
}
