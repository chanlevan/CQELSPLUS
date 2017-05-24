package cqelsplus.execplan.data;

import java.math.BigInteger;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.NodeValue;

import cqelsplus.engine.ExecContextFactory;
import cqelsplus.execplan.indexes.SortedMap;


public class DomMinEntry extends DomAggEntry{
	Number min;
	SortedMap<Number> map;
	public DomMinEntry() {
		map=ExecContextFactory.current().engine().sortedMapGen(POOL.DomEntry);
	}
	
	public void update(IMapping mu){
		//TODO with query context
		NodeValue nv = NodeValue.makeNode(ExecContextFactory.current().engine().decode(mu.getValue(acc.accVar())));
		if (nv.isFloat()) {
			Float idx = nv.getFloat();
			DomEntry entry = map.get(idx);
			if(entry==null)
			{
				if(idx < ((Float)min)) min=idx;
				entry=(DomEntry)POOL.DomEntry.borrowObject();
				entry.reset();
				map.put(idx, entry);
			}
			else entry.incCount();
		} else if (nv.isInteger()){
			BigInteger idx = nv.getInteger();
			DomEntry entry = map.get(idx);
			if(entry==null)
			{
				if(idx.compareTo(((BigInteger)min)) < 0) min=idx;
				entry=(DomEntry)POOL.DomEntry.borrowObject();
				entry.reset();
				map.put(idx, entry);
			}
			else entry.incCount();
			
		}
	}
	
	public void expire(IMapping mu) { 
		//TODO withc query context
		NodeValue nv = NodeValue.makeNode(ExecContextFactory.current().engine().decode(mu.getValue(acc.accVar())));
		if (nv.isFloat()) {
			Float idx = nv.getFloat();
			DomEntry entry=map.get(idx);
			if(idx == min && entry.count()==1) {
				map.remove(idx, mu.getDataItem());
				if(!map.isEmpty()) min = (Float)map.getFirst();
				else min= Float.MAX_VALUE;
				return;	
			}
			entry.decrCount();
		} else if (nv.isInteger()) {
			BigInteger idx = nv.getInteger();
			DomEntry entry=map.get(idx);
			if(idx.compareTo(((BigInteger)min)) == 0 && entry.count()==1) {
				map.remove(idx, mu.getDataItem());
				if(!map.isEmpty()) min = (BigInteger)map.getFirst();
				else min= BigInteger.valueOf(Long.MAX_VALUE);
				return;	
			}
			entry.decrCount();			
		}
	}
	
	@Override
	public void reset(IMapping mu) {
		NodeValue nv = NodeValue.makeNode(ExecContextFactory.current().engine().decode(mu.getValue(acc.accVar())));
		if (nv.isFloat()) {
			min = Float.MAX_VALUE;
		} else if (nv.isInteger()) {
			min = BigInteger.valueOf(Long.MAX_VALUE);
		}
		update(mu);
		reset();
	}
	
	public long accVal() {
		if (min instanceof Float) {
			Node acc = Node.createLiteral(((Float)min).toString(), null, XSDDatatype.XSDfloat);
			return ExecContextFactory.current().engine().encode(acc);
		}
		if (min instanceof BigInteger) {
			Node acc = Node.createLiteral(((BigInteger)min).toString(), null, XSDDatatype.XSDinteger);
			return ExecContextFactory.current().engine().encode(acc);
		}
		return -1;
	}

}
