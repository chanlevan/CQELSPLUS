package cqelsplus.execplan.indexes;

public abstract class SortedMap<T> extends DomainIndex<T> {
	public abstract T getFirst();
	public abstract T getLast();
	public abstract boolean isEmpty();

}
