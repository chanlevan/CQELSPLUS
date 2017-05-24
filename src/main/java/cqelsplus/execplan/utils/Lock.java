package cqelsplus.execplan.utils;

public class Lock {
	private boolean isLocked = false;
	public synchronized void lock() {
		while(isLocked) {
			try {
				wait();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		isLocked = true;
	}
	
	public synchronized void unlock() {
		isLocked = false;
		notify();
	}
	
	public boolean isLocking() {
		return isLocked;
	}
}
