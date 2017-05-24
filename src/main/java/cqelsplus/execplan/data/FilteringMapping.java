package cqelsplus.execplan.data;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

import cqelsplus.engine.ExecContextFactory;

public class FilteringMapping implements Binding {

	IMapping base;
	public FilteringMapping() {}

	public void set( IMapping base) {
		this.base = base;
	}
	
	public void unset() {
		this.base = null;		
	}
	
	public List<Var> getVars() {
		return base.getVars();
	}
	
	@Override
	public Iterator<Var> vars() {
		return base.getVars().iterator();
	}

	@Override
	public boolean contains(Var var) {
		// TODO Auto-generated method stub
		return base.getVars().contains(var);
	}
	
	@Override
	public Node get(Var var) {
		long value = base.getValue(var);
		if (value != -1) {
			return ExecContextFactory.current().engine().decode(value);
		}
		return null;	
	}
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return base.getVars().size();
	}
	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return (this.base.getVars() == null || base.getVars().isEmpty());
	}
}
