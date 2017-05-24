package cqelsplus.execplan.data;

public class DomEntry implements PoolableObject,LinkedItem {
	protected Integer count;
	public DomEntry()
	{
		count=0;
	}
	
	public Integer count() 
	{
		return count;
	}
	public synchronized void setCount(int count)
	{ 
		this.count=count;
	}
	public synchronized void incCount()
	{ 
		count++;
	};
	public synchronized void decrCount()
	{ 
		count--;
	};
	public synchronized void reset()
	{ 
		count=1;
	};
	public PoolableObject newObject() 
	{
		return (PoolableObject)POOL.DomEntry.borrowObject();
	}
	
	public void releaseInstance() {
		count = 0;
		POOL.DomEntry.returnObject(this);
	}

	public LinkedItem getLink() 
	{
		return null;
	}

	public void setLink(LinkedItem item) {		
	}
}
