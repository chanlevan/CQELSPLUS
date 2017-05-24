package cqelsplus.engine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.data.IMapping;

public abstract class ConstructListener implements ContinousListener {
	Query query;
	ExecContext context;
	public ConstructListener(Query query, ExecContext context) { 
		this.query = query;
		this.context = context;
	}
	
	public abstract void update(List<Triple> graph);
	@Override
	public void update(IMapping mapping) {
		List<Triple> triples = query.getConstructTemplate().getTriples();
		Iterator<Triple> ti = triples.iterator();
		ArrayList<Triple> graph = new ArrayList<Triple>();
		while(ti.hasNext()) {
			Triple triple = ti.next();
			Node s, p, o;
			
			if (triple.getSubject().isVariable()) {
				s = context.engine().decode(mapping.getValue(Var.alloc(triple.getSubject())));
			}
			else {
				s = triple.getSubject();
			}
			
			if (triple.getPredicate().isVariable()) {
				p = context.engine().decode(mapping.getValue(Var.alloc(triple.getPredicate())));
			}
			else {
				p = triple.getPredicate();
			}
			
			if (triple.getObject().isVariable()) {
				o = context.engine().decode(mapping.getValue(Var.alloc(triple.getObject())));
			}
			else { 
				o = triple.getObject();
			}
			graph.add(new Triple(s, p, o));
		}
		update(graph);

	}
}
