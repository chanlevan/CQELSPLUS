package cqelsplus.execplan.data.iterator;

import java.util.Iterator;

import cqelsplus.execplan.data.ITuple;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.data.MultipleLinks;

public class RingIterator implements Iterator<ITuple> {
	LeafTuple start,cur;
	long startTick,endTick;
	int linkId,count,visited;
	public RingIterator(LeafTuple start, long startTick, long endTick, int linkId, int count) {
		this.start = start;
		this.startTick = startTick;
		this.endTick = endTick;
		this.linkId = linkId;
		this.count = count;
		rewind();
	}
	
	public void rewind(){
		cur = start;
		visited = 0;
		move();
	}
	
	public boolean hasNext() {
		if(visited >= count || cur == null || cur.timestamp < endTick) return false;
		return true;
	}
	
	public LeafTuple getLink(ITuple item){
		if(item.getLink() instanceof MultipleLinks)
			return(LeafTuple)((MultipleLinks)cur.getLink()).getLink(linkId);
		else return (LeafTuple)cur.getLink();
	}

	public void move(){
		while (cur != null && startTick < cur.timestamp && visited < count) {
			cur = getLink(cur);
			visited++;
		}
	}
	
	public ITuple next() {
		LeafTuple tmp = cur;
		cur = getLink(cur);
		visited++;
		return tmp;
	}

	public void remove() {
		// TODO Auto-generated method stub

	}

}
