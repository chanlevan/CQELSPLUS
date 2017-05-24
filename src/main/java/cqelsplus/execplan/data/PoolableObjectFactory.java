package cqelsplus.execplan.data;

import java.util.concurrent.LinkedBlockingDeque;

public abstract class PoolableObjectFactory{
	LinkedBlockingDeque<Object> pool;
	public PoolableObjectFactory(){
		pool=new LinkedBlockingDeque<Object>();
	}
	public  Object borrowObject(){
		/*if(pool.size()==0) {
			int i=globalPool.size();
			while(i>0&&threadObjPool.size()==0){
			globalPool.add(threadObjPool);
			threadObjPool=globalPool.poll();
			i--;
			}
		}*/
		synchronized (pool) {
			if(pool.size()>0) {
					//System.out.println("POOL size of " + this.getClass().toString() + ": " + pool.size());
					return pool.poll();
			}
			return newObject();
		}

	}
	public abstract Object newObject();
	public void returnObject(Object obj){
		pool.add(obj);
	}
	
	public int getPoolSize() {
		return pool.size();
	}
	
}
