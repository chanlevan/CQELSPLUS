package cqelsplus.execplan.data;

public interface PoolableObject {
	public PoolableObject newObject();
	public void releaseInstance();
}
