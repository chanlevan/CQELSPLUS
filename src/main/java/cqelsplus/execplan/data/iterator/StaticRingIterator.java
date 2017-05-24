package cqelsplus.execplan.data.iterator;

import cqelsplus.execplan.data.LeafTuple;

public class StaticRingIterator extends RingIterator {
	public StaticRingIterator(LeafTuple start, int linkId, int count) {
		super(start, Long.MAX_VALUE, Long.MIN_VALUE, linkId, count);
	}

}
