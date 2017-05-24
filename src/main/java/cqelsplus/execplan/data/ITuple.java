package cqelsplus.execplan.data;


import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.mjoinnetwork.ProbingSequence;
/**DI = data item, i.e encoded streamed data*/
public interface ITuple extends LinkedItem, RoutingMessage {
	/**get value at particular position idx*/
	public long get(int idx);
	/**get value bound with a variable in 1 specific context of probing 
	 * See more ProbingContext class for details !*/
	public long getByVar(Var var, ProbingSequence ps);
	/**indicate whether this data item is expired in this probing context
	 * See more ProbingContext class for details !*/
	public boolean isExpired(ProbingSequence ps);
}
