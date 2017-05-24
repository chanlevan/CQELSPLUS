package cqelsplus.queryparser;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;

public class ElementStreamGraph extends ElementNamedGraph {
	private Window window;
	public ElementStreamGraph(Node n, Window w, Element el) {
		super(n, el);
		window = w;
	}
	
	public Window getWindow() {	return window; }
}
