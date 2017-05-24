package cqelsplus.logicplan;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;

public class CompilerFactory {
	public static void compile(Element elt) {
        if ( elt instanceof ElementGroup ) {
            compileElementGroup((ElementGroup)elt);
        	return;
		}
      
        if ( elt instanceof ElementUnion ) {
            compileElementUnion((ElementUnion)elt);
            return;
        }
      
        if ( elt instanceof ElementSubQuery ) {
            compileElementSubquery((ElementSubQuery)elt);
            return;
        }
	}
	
	public static void compileElementGroup(ElementGroup elt){}
	public static void  compileElementUnion(ElementUnion elt){}
	public static void compileElementGraph(ElementNamedGraph elt){}
	public static void compileElementSubquery(ElementSubQuery elt){}
	public static void compileModifiers(Query query){}
}
