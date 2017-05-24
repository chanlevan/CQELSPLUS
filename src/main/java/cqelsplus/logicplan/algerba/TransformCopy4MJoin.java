package cqelsplus.logicplan.algerba;

import java.util.List;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.op.OpN;

public class TransformCopy4MJoin extends TransformCopy {
     boolean alwaysCopy = false;
	 public Op transform(OpMJoin opMJoin, List<Op> elts) {
		  return xform(opMJoin, elts) ;
	 }
	 
    private Op xform(OpN op, List<Op> elts) {
        // Need to do one-deep equality checking.
        if ( ! alwaysCopy && equals1(elts, op.getElements()) )
            return op ;
        return op.copy(elts) ;
    }
    
    private boolean equals1(List<Op> list1, List<Op> list2) {
        if ( list1.size() != list2.size() )
            return false ;
        for ( int i = 0 ; i < list1.size() ; i++ )
        {
            if ( list1.get(i) != list2.get(i) )
                return false ;
        }
        return true ;
    }
}
