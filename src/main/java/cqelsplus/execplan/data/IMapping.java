package cqelsplus.execplan.data;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.mjoinnetwork.ProbingSequence;

/**
 * This is the class that represents 1 mapping after MJOIN which is belonging to a specific probing context
 * */
public interface IMapping {
	List<Var> getVars();
	public long getValue(Var var); 
	public ArrayList<ITuple> getLeaveTuples();
	public ITuple getDataItem();
	public boolean contains(Cont_Dep_ExpM leaf);
	public ProbingSequence getProbingSequence();
};
