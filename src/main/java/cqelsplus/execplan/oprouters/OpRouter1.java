package cqelsplus.execplan.oprouters;

import com.hp.hpl.jena.sparql.algebra.Op;

/** 
 * This abstract class has characteristic of a unary router 
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see OpRouterBase
 */
public abstract class OpRouter1 extends OpRouterBase {
	protected OpRouter subRouter;
	public OpRouter1(Op op) {
		super(op);
	}
	
	public void addSubRouter(OpRouter subRouter) {
		this.subRouter = subRouter;
	}
	
	public OpRouter getSubRouter() {
		return this.subRouter;
	}
}
