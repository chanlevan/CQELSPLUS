package cqelsplus.execplan.data;

import java.util.Iterator;

public class BatchBuff implements PoolableObject {
	MappingEntry head,last;

	public BatchBuff(){
		head = last = null;
	}

	public void insert(IMapping m) {
		MappingEntry entry = (MappingEntry)POOL.MuEntry.borrowObject();
		entry.set(m, head, null);
		head = entry;
		if(last == null) {
			last = head;
		}
	}
	
	public void insert(MappingEntry entry) {
		entry.next = head;
		head.prev = entry;
		head = entry;
		if(last == null) {
			last = head;
		}
	}
	
	public void addBuff(BatchBuff other) {
		if (head == null) {
			head=other.getFirst();
			last=other.getLast();
		} else {
			last.next = other.getFirst();
			if (last.next != null) {
				last.next.prev = last;
			}
			last = other.getLast();
		}
	}
	
	public Iterator<MappingEntry> iterator(){
		return new MUEntryIter(head);
	}
	
	public MappingEntry getFirst(){ 
		return head;
	}
	
	public MappingEntry getLast() { 
		return last;
	}

	public boolean isEmpty() {
		return (head == null);
	}
	
	public void remove(MappingEntry entry) {
		if (entry.next != null) {
			entry.next.prev = entry.prev;
		} else {
			last = entry.prev;
		}
		if (entry.prev != null) {
			entry.prev.next = entry.next;
		} else {
			head = entry.next;
		}
		
	}
	
	public void clear() {
		head = last = null;
	}

	public static class MUEntryIter implements Iterator<MappingEntry>{
		MappingEntry cur;
		public MUEntryIter(MappingEntry head){
			cur = head;
		}
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return cur != null;
		}

		public MappingEntry next() {
			MappingEntry tmp = cur;
			cur = cur.next;
			// TODO Auto-generated method stub
			return tmp;
		}

		public void remove() {}
		
	}

	@Override
	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseInstance() {
		//testReleaseTotal();
		this.clear();
		POOL.BatchBuff.returnObject(this);		
	}
	
	public void testReleaseTotal()  {
		MappingEntry p = head;
		while (p != null) {
			MappingEntry tmp = p;
			tmp.releaseInstance();
			p = p.next;
		}
	}
	
	String signal;
	public void setSignal(String signal) {
		this.signal = signal;
	}
	
	public String getSignal() {
		return this.signal;
	}
}
