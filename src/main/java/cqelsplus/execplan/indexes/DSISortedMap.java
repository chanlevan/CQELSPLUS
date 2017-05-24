package cqelsplus.execplan.indexes;

//import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
//import it.unimi.dsi.fastutil.objects.Object2ObjectRBTreeMap;
//import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import cqelsplus.execplan.data.DomEntry;
import cqelsplus.execplan.data.PoolableObjectFactory;

public class  DSISortedMap<T> extends SortedMap<T> {
	protected Map<T, DomEntry> index;
	protected int combinedSize;
	public DSISortedMap(PoolableObjectFactory entryFactory) {
		index =new HashMap<T, DomEntry>();
		expiredKeys=new ArrayDeque<T>();
		this.entryFactory=entryFactory;
	}
	
//	public DSISortedMap(int type,PoolableObjectFactory entryFactory){
//		this.entryFactory=entryFactory;
//		expiredKeys=new ArrayDeque<T>();
//		if(type==1) index =new Object2ObjectAVLTreeMap<T, DomEntry>();
//		else if(type==2)
//			index =new Object2ObjectRBTreeMap<T, DomEntry>();
//		else
//			index =new HashMap<T, DomEntry>();
//	}
	 
	public DSISortedMap(int type,PoolableObjectFactory entryFactory){
		this.entryFactory=entryFactory;
		expiredKeys=new ArrayDeque<T>();
		if(type==1) index =new TreeMap<T, DomEntry>();
		else
			index =new HashMap<T, DomEntry>();
	}
	 
	
	public DomEntry get(T idx) {
		DomEntry entry = null;
		try {
			entry = index.get(idx);
		} catch (Exception e) {
			System.out.print(index.size() + " : " + idx);
			e.printStackTrace();
		}
		return entry;
	}

	public synchronized void put(T idx, DomEntry entry) {
		index.put(idx, entry);
	}
	@Override
	protected synchronized void _remove(T idx) {
		DomEntry entry=index.remove(idx);
		entry.releaseInstance();
	}
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return index.size();
	}
	 
	public Iterator<T> keys(){
		return index.keySet().iterator();
	}
	
	@Override
	public void release() {
		index.clear();
	}
	@Override
	public boolean isEmpty(){ return index.isEmpty();}
	
//	@Override
//	public T getFirst(){ return ((Object2ObjectSortedMap<T, DomEntry>)index).firstKey();}
//	
//	@Override
//	public T getLast(){ return ((Object2ObjectSortedMap<T, DomEntry>)index).lastKey();}
	
	@Override
	public T getFirst(){ return ((TreeMap<T, DomEntry>)index).firstKey();}
	
	@Override
	public T getLast(){ return ((TreeMap<T, DomEntry>)index).lastKey();}

}
