package cqelsplus.execplan.oprouters;

import com.hp.hpl.jena.sparql.algebra.Op;


/** *This interface contains the behaviours of a router which is responsible to receive a mapping and route it
 *to the higher node in the router-node tree 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public interface OpRouter {
	/**
	 * Each specific router contains an operator what is parsed from the query
	 * This method will get that op out 
	 */
	public Op getOp();
	public void routeNewArrival(Object object);
	public void routeExpiration(Object object);
	public int getId();
	public void addNextRouter(OpRouter nextRouter);
	public OpRouter getNextRouter();
}
