package cqelsplus.execplan.indexes;

import java.util.ArrayDeque;
import java.util.Iterator;

import cqelsplus.execplan.data.DomEntry;
import cqelsplus.execplan.data.ITuple;
import cqelsplus.execplan.data.LinkedItem;
import cqelsplus.execplan.data.MultipleLinks;
import cqelsplus.execplan.data.PoolableObjectFactory;

public abstract class DomainIndex<T> {
	static final int purgeRatio=25;
	protected ArrayDeque<T> expiredKeys;
	public abstract DomEntry get(T idx);
	protected PoolableObjectFactory entryFactory;
	public abstract void put(T idx, DomEntry entry);
	
	public abstract int size();
	public abstract Iterator<T> keys();
	protected abstract void _remove(T idx);
	public boolean isEmpty() { return (size() == 0);};
	public void add(int idCol, T idxKey, ITuple mu){
		//note: mu must be set a skeleton link
		DomEntry entry=get(idxKey);
		if(entry == null) {
			entry =(DomEntry)entryFactory.borrowObject();
			entry.reset();
			put(idxKey,entry);
		}
		else 
			entry.incCount();
		if(mu.getLink()==null) return;
		
		//Just one index, use the link of the mapping for linking the same key 
		if(mu.getLink() instanceof ITuple){
			LinkedItem tmp = entry.getLink();
			mu.setLink(tmp);
			entry.setLink(mu);
			return;
		}
		//for two indexes and more, use intermediate links
		if(mu.getLink() instanceof MultipleLinks){
			LinkedItem tmp = entry.getLink();			
			((MultipleLinks)mu.getLink()).setLink(idCol, tmp);
			entry.setLink(mu);
			return;
		}
	}
	
	public boolean purge() {
		boolean purged=false;
		if(!expiredKeys.isEmpty()){
			DomEntry entry=get(expiredKeys.peek());
			if(entry!=null&&entry.count()==0){
				_remove(expiredKeys.poll());
				purged=true;	
			}
			expiredKeys.poll();
		}
		
		return purged;	
	}
	
	public void remove(T key, ITuple mu){
		DomEntry entry=get(key);
		if (entry != null) {//should be checked if 2 virtual windows refer to the same physical windowgit 
			entry.decrCount();
			if(entry.count()<=0) _remove(key);
		}
		//Don't need to update links from the domain value
	}
	public void remove(T key){
		expiredKeys.add(key); 
		while(expiredKeys.size()*100/size()>purgeRatio){
			purge();
		}
	}
	
	public void release(){
		//TODO
	}
}
