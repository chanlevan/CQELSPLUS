package cqelsplus.execplan.data;

import cqelsplus.execplan.windows.PhysicalWindow;

public abstract class BaseTuple implements ITuple, PoolableObject, LinkedItem {
	protected PhysicalWindow from;

	protected MappingEntry entry;


	@Override
	public boolean equals(Object obj) {
		///TODO 
		return false;
	}
	
	public void setBatchEntry(MappingEntry entry) {
		this.entry = entry;
	}
	
	public MappingEntry getBatchEntry() {
		return this.entry;
	}

	public PhysicalWindow getFrom() {		
		return from; 
	}	
	
	public void setFrom(PhysicalWindow from){ 
		this.from=from;
	}

	public LinkedItem getLink() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void setLink(LinkedItem item) {
		// TODO Auto-generated method stub	
	}
}
