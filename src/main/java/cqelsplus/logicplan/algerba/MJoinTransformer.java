package cqelsplus.logicplan.algerba;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.Transform;
import com.hp.hpl.jena.sparql.algebra.op.Op0;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpN;
/**
 * This class is in charge of transforming a logical binary-join-including query plan
 * into a logical M-join-including query plan
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
 
public class MJoinTransformer {
	private static MJoinTransformer mjoinTransformer = null;
	public MJoinTransformer() {}
//	public static MJoinTransformer getMJoinTransformer() {
//		if (mjoinTransformer == null) {
//			mjoinTransformer = new MJoinTransformer();
//		}
//		return mjoinTransformer;
//	}
//	
	private boolean needToReVisit;
	
	Deque<Op> stack = new ArrayDeque<Op>();
	
	Transform tf = new TransformCopy4MJoin();
	/**
	 * Transform a binary-including-join op to a MJoin-including op 
	 * @param queryOp 
	 */
	public Op transform(Op queryOp) {
		
		stack.push(queryOp);	
		
		do {
			
			needToReVisit = false;
			
			trailVisit(stack.pop());
		
		} while (needToReVisit);
		
		return stack.pop();
		
	}
	/**
	 * Visit Op tree to build MJoin
	 * @param current node
	 */
	public void trailVisit(Op current) {
		if (current instanceof OpJoin) {
			OpMJoin newMJoin = new OpMJoin((OpJoin)current);
			stack.push(newMJoin);
			
			needToReVisit = true;
			
		} else {
			if (current instanceof Op0) {
				stack.push(current);
			}
			if (current instanceof Op1) {
				trailVisit(((Op1)current).getSubOp());
				
	            Op subOp = null;
	            if (((Op1)current).getSubOp() != null)
	                subOp = stack.pop();
	            stack.push(((Op1) current).apply(tf, subOp));
				
			} else if (current instanceof Op2) {

				trailVisit(((Op2)current).getLeft());
				trailVisit(((Op2)current).getRight());
				
	            Op left = null;
	            Op right = null;
	    
	            // Must do right-left because the pushes onto the stack were left-right. 
	            if (((Op2)current).getRight() != null)
	                right = stack.pop() ;
	            if (((Op2)current).getLeft() != null)
	                left = stack.pop() ;
	            Op opX = ((Op2)current).apply(tf, left, right) ; 
	            stack.push(opX) ;
			
			} else if (current instanceof OpN) {
				for (int i = 0; i < ((OpN)current).size(); i++) {
					trailVisit(((OpN)current).get(i));
				}
				
	            List<Op> x = new ArrayList<Op>(((OpN)current).size()) ;
	            for ( Iterator<Op> iter = ((OpN)current).iterator(); iter.hasNext(); ) {
	                Op sub = iter.next() ;
	                Op r = stack.pop() ;
	                // Skip nulls.
	                if ( r != null )
	                    // Add in reverse.
	                    x.add(0, r) ;
	            }
	            Op opX = ((OpN)current).apply(tf, x) ;  
	            stack.push(opX) ;				
			}
		}
	}
}
