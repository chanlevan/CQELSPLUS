package cqelsplus.logicplan.algerba;

import java.util.Arrays;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Quad;

import cqelsplus.queryparser.Window;

public class OpStream extends OpQuadPattern {
	Window window;
	Quad quad;
	boolean special;
	int specialId;
	static int count=0;
	public OpStream(Node node,BasicPattern pattern, Window window) {
		super(node, pattern);
		this.window = window;
		this.quad = new Quad(this.getGraphNode(), this.getBasicPattern().get(0));
	}
	
	public OpStream(Node node, Triple triple, Window window) {
		this(node, BasicPattern.wrap(Arrays.asList(triple)), window);
		special = false;
	}
	
	public Window getWindow() {
		return window;
	}
	
	public Quad getQuad() {
		return this.quad;
	}
	
	public void setSpecial() {
		special = true;
		specialId = ++count;
	}
	
	public int getSpecialId() {
		return specialId;
	}
	
	public boolean isSpecial() {
		return special;
	}
	/*
	public void vars(Set<Var> acc){
		addVar(acc,getGraphNode());
		addVarsFromTriple(acc, ((OpTriple)getSubOp()).getTriple());
	}
	
	private static void addVarsFromTriple(Collection<Var> acc, Triple t)
	{
	        addVar(acc, t.getSubject()) ;
	        addVar(acc, t.getPredicate()) ;
	        addVar(acc, t.getObject()) ;
	}
	private static void addVar(Collection<Var> acc, Node n)
    {
        if ( n == null )
            return ;
        
        if ( n.isVariable() )
            acc.add(Var.alloc(n)) ;
    }*/

}
