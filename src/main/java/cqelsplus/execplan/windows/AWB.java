package cqelsplus.execplan.windows;

import java.util.ArrayList;
import java.util.Iterator;

import cqelsplus.execplan.data.ITuple;

public interface AWB {
	public void insert(ITuple m);
	public Iterator<ITuple> probe(int indexVal, ITuple mu, ArrayList<Integer> prevVarsPos, long tick);
	public void purge(ITuple firstInvalidItem);
}
