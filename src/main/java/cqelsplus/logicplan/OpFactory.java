package cqelsplus.logicplan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;

import cqelsplus.logicplan.algerba.OpStream;
import cqelsplus.queryparser.Window;

public class OpFactory {
  	public static void addStaticOp(ArrayList<OpQuadPattern> staticOps, ElementNamedGraph el) {
  		
 	 	if(el.getElement() instanceof ElementTriplesBlock) {
 	 		
 	 		addStaticOp(staticOps, ((ElementTriplesBlock)el.getElement()).getPattern().getList(), el.getGraphNameNode());
 	 	}
 	 	else if(el.getElement() instanceof ElementGroup) {
 	 		
 	 		addStaticOp(staticOps, (ElementGroup)el.getElement(), el.getGraphNameNode());
 	 	}
 	 	else {
 	 		System.out.println("Stream pattern is not ElementTripleBlock" + el.getElement().getClass());
 	 	}
  	}
  	
  	public static void addStaticOp(ArrayList<OpQuadPattern> staticOps, ElementGroup group, Node graphNode) {
 		for(Element el:group.getElements()) {
 			
 			if(el instanceof ElementTriplesBlock) {
 				
 				addStaticOp(staticOps, ((ElementTriplesBlock)el).getPattern().getList(), graphNode);
 				
 			}
 			if(el instanceof ElementPathBlock) {
 				
 				for(Iterator<TriplePath> paths = ((ElementPathBlock)el).patternElts(); paths.hasNext(); ) {
 					
 					Triple t = paths.next().asTriple();
 					
 					if(t != null)
 						
 						staticOps.add(new OpQuadPattern(graphNode, BasicPattern.wrap(Arrays.asList(t))));
 					
 					else {
 						System.out.println("Path is not supported");
 					}
 				}
 			}
 			else {
 				System.out.println("unrecognized block" + el.getClass());
 			}
 		}
 	}
     
  	public static void addStaticOp(ArrayList<OpQuadPattern> staticOps, ElementPathBlock block, Node graphNode) {
			for(Iterator<TriplePath> paths = block.patternElts(); paths.hasNext(); ) {
					Triple t = paths.next().asTriple();
					if(t != null)
						staticOps.add(new OpQuadPattern(graphNode, BasicPattern.wrap(Arrays.asList(t))));
					else {
						System.out.println("Path is not supported");
					}
				}

 	}    

  	public static void addStaticOp(ArrayList<OpQuadPattern> staticOps, List<Triple> list, Node graphNode) {
 		for(Triple t :list) {
 			staticOps.add(new OpQuadPattern(graphNode, BasicPattern.wrap(Arrays.asList(t))));
 		}
 	}     
    
  	public static void addStreamOp(ArrayList<OpStream> streamOps, ElementTriplesBlock el, Node graphNode, Window window) {
		
		for(Triple t:el.getPattern().getList()) {
			
			streamOps.add(new OpStream(graphNode,t,window));
		}
	}
	
  	public static void addStreamOp(ArrayList<OpStream> streamOps, ElementGroup group, Node graphNode, Window window) {
    	
		for(Element el:group.getElements()) {
		
			if(el instanceof ElementTriplesBlock) {
				
				addStreamOp(streamOps, (ElementTriplesBlock)el, graphNode, window);
				
			}
			if(el instanceof ElementPathBlock) {
				
				for(Iterator<TriplePath> paths = ((ElementPathBlock)el).patternElts(); paths.hasNext(); ) {
					
					Triple t = paths.next().asTriple();
					
					if(t != null)
					
						streamOps.add(new OpStream(graphNode, t, window));
					
					else {
					
						System.out.println("Path is not supported");
						
					}
				}
			}
			else {
				
				System.out.println("unrecognized block" + el.getClass());
				
			}
		}
	}
  	
  	public static Op buildOp(ArrayList<OpQuadPattern> staticOps) {
  		if (staticOps.size() == 0)
  			return null;
  		
  		if (staticOps.size() == 1)
  			return staticOps.get(0);
  		
  		Op current = staticOps.get(0);
  		for (int i = 1; i < staticOps.size(); i++) {
  			current = OpJoin.create(current, staticOps.get(i));
  		}
  		
  		return current;
  	}
}
