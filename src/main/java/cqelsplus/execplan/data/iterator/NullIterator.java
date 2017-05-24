package cqelsplus.execplan.data.iterator;

import java.util.Iterator;

import cqelsplus.execplan.data.ITuple;

public class NullIterator implements Iterator<ITuple>{
	private static NullIterator instance;
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ITuple next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}
	
	public static NullIterator instance() { 
		if (instance == null) {
			instance = new NullIterator();
		}
		return instance;
	}
}
