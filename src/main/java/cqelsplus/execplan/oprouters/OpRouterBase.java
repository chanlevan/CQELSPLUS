package cqelsplus.execplan.oprouters;

import com.hp.hpl.jena.sparql.algebra.Op;

/**
 *This class implements the basic behaviors of a router
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see OpRouter
 */

public abstract class OpRouterBase implements OpRouter {
	Op op;
	/** An execution context that the router is working on */
	int id;
	protected OpRouter nextRouter; 
	
	public OpRouterBase(Op op) {
		this.op = op;
		//System.out.println("new op "+op);
	}
	
	public Op getOp() {
		// TODO Auto-generated method stub
		return op;
	}
	
	public int getId() { 
		return id;
	}
	
	public void addNextRouter(OpRouter nextRouter) {
		this.nextRouter = nextRouter;
	}
	
	public OpRouter getNextRouter(){
		return this.nextRouter;
	}
}
