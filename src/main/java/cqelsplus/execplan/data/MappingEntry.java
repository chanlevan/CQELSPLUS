package cqelsplus.execplan.data;

public class MappingEntry implements PoolableObject {
	private IMapping elm = null;
	public MappingEntry next;
	public MappingEntry prev;
	
	MappingEntry(){
		next = prev = null;
	};
	
	public void set(IMapping elm, MappingEntry next, MappingEntry prev){
		this.elm = elm;
		this.next = next;
		this.prev = prev;

		if (next != null) {
			next.prev = this;
		}
	
		if (prev != null) {
			prev.next = this;
		}
	}
	
	private void unset() {
		elm = null;
		next = null;
		prev = null;
	}
	
	public IMapping getElm() {
		return elm;
	}

	@Override
	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseInstance() {
		unset();
		POOL.MuEntry.returnObject(this);
	}
}