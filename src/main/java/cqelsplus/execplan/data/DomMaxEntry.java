package cqelsplus.execplan.data;

import java.math.BigInteger;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.NodeValue;

import cqelsplus.engine.ExecContextFactory;
import cqelsplus.execplan.indexes.SortedMap;

public class DomMaxEntry extends DomAggEntry{
	Number max;
	SortedMap<Number> map;
	public DomMaxEntry() {
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
				if(idx > ((Float)max)) max=idx;
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
				if(idx.compareTo(((BigInteger)max)) > 0) max=idx;
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
			if(idx == max && entry.count()==1) {
				map.remove(idx, mu.getDataItem());
				if(!map.isEmpty()) max = (Float)map.getFirst();
				else max= Float.MIN_VALUE;
				return;	
			}
			entry.decrCount();
		} else if (nv.isInteger()) {
			BigInteger idx = nv.getInteger();
			DomEntry entry=map.get(idx);
			if(idx.compareTo(((BigInteger)max)) == 0 && entry.count()==1) {
				map.remove(idx, mu.getDataItem());
				if(!map.isEmpty()) max = (BigInteger)map.getFirst();
				else max= BigInteger.valueOf(Long.MIN_VALUE);
				return;	
			}
			entry.decrCount();			
		}
	}
	
	@Override
	public void reset(IMapping mu) {
		NodeValue nv = NodeValue.makeNode(ExecContextFactory.current().engine().decode(mu.getValue(acc.accVar())));
		if (nv.isFloat()) {
			max = Float.MIN_VALUE;
		} else if (nv.isInteger()) {
			max = BigInteger.valueOf(Long.MIN_VALUE);
		}
		update(mu);
		reset();
	}
	
	public long accVal() {
		if (max instanceof Float) {
			Node acc = Node.createLiteral(((Float)max).toString(), null, XSDDatatype.XSDfloat);
			return ExecContextFactory.current().engine().encode(acc);
		}
		if (max instanceof BigInteger) {
			Node acc = Node.createLiteral(((BigInteger)max).toString(), null, XSDDatatype.XSDinteger);
			return ExecContextFactory.current().engine().encode(acc);
		}
		return -1;
	}	
}
