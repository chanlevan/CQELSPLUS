package cqelsplus.execplan.data;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.oprouters.MJoinRouter;
import cqelsplus.execplan.queue.EventQueue;

public class POOL {

	public static PoolableObjectFactory DomAggEntryMap=new PoolableObjectFactory() {
		
		@Override
		public Object newObject() {
			return new HashMap<ITuple, DomAggEntry[]>();
		}
		
		public void returnObject(Object obj){
			((HashMap<ITuple, DomAggEntry[]>)obj).clear();
			super.returnObject(obj);
		}
		
	};
	
	public static PoolableObjectFactory CONT_DEP_MU = new PoolableObjectFactory() {
		@Override
		public Object newObject() {
			return new Cont_Dep_M();
		}
	};
	
	public static PoolableObjectFactory CONT_DEP_EXPMU = new PoolableObjectFactory() {
		@Override
		public Object newObject() {
			return new Cont_Dep_ExpM();
		}
	};
	
	public static PoolableObjectFactory CONT_DEP_EXPMU_LIST = new PoolableObjectFactory() {
		@Override
		public Object newObject() {
			return new ArrayList<Cont_Dep_ExpM>();
		}
	};
	
	
	public static PoolableObjectFactory DomEntryMap=new PoolableObjectFactory() {
		
		@Override
		public Object newObject() {
			return new HashMap<ITuple, DomEntry>();
		}
		
		public void returnObject(Object obj){
			((HashMap<ITuple, DomEntry>)obj).clear();
			super.returnObject(obj);
		}
		
	};
	
	public static PoolableObjectFactory DomMappingEntryMap=new PoolableObjectFactory() {
		
		@Override
		public Object newObject() {
			return new HashMap<IMapping, DomEntry>();
		}
		
		public void returnObject(Object obj){
			((HashMap<IMapping, DomEntry>)obj).clear();
			super.returnObject(obj);
		}
		
	};
	
	public static PoolableObjectFactory DomAggEntry=new PoolableObjectFactory() {
		
		@Override
		public Object newObject() {
			return new DomAggEntry();
		}
	};
	
public static PoolableObjectFactory DomSumEntry=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new DomSumEntry();
	}
};
	
public static PoolableObjectFactory DomAVGEntry=new PoolableObjectFactory() {
		
		@Override
		public Object newObject() {
			return new DomAVGEntry();
		}
	};
	

	public static PoolableObjectFactory DomMinEntry=new PoolableObjectFactory() {
		
		@Override
		public Object newObject() {
			return new DomMinEntry();
		}
	};
	
	public static PoolableObjectFactory DomMaxEntry=new PoolableObjectFactory() {
		
		@Override
		public Object newObject() {
			return new DomMaxEntry();
		}
	};

public static PoolableObjectFactory MUN=new PoolableObjectFactory() {
	@Override
	public Object newObject() {
		return new LeafTuple();
	}
};

public static PoolableObjectFactory MUJ=new PoolableObjectFactory() {
	@Override
	public Object newObject() {
		return new InterJoinTuple();
	}
};

public static PoolableObjectFactory MUA = new PoolableObjectFactory() {
	@Override
	public Object newObject() {
		return new AggMapping();
	}
};

public static PoolableObjectFactory MUE = new PoolableObjectFactory() {
	@Override
	public Object newObject() {
		return new ExtendMapping();
	}
};

public static PoolableObjectFactory MUP = new PoolableObjectFactory() {
	@Override
	public Object newObject() {
		return new MUP();
	}
};

public static PoolableObjectFactory DomEntry=new PoolableObjectFactory() {
	@Override
	public Object newObject() {
		return new DomEntry();
	}
};

public static PoolableObjectFactory Probing=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		// TODO Auto-generated method stub
		return new Join();
	}
};

public static PoolableObjectFactory BatchBuff=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		// TODO Auto-generated method stub
		return new BatchBuff();
	}
};

public static PoolableObjectFactory DomRingEntry=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new DomRingEntry();
	}
};

public static PoolableObjectFactory TwoLinks=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new TwoLinks();
	}
};
public static PoolableObjectFactory TwoArray=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new long[2];
	}
};
public static PoolableObjectFactory ThreeArray=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new long[3];
	}
};

public static PoolableObjectFactory OneArray=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new long[1];
	}
};

public static PoolableObjectFactory MuList =new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new ArrayList<LeafTuple>();
	}
	
	public void returnObject(Object obj){
		((ArrayList<LeafTuple>)obj).clear();
		super.returnObject(obj);
	}
};

public static PoolableObjectFactory MUNList =new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new ArrayList<LeafTuple>();
	}
	
	public void returnObject(Object obj){
		((ArrayList<LeafTuple>)obj).clear();
		super.returnObject(obj);
	}
};

public static PoolableObjectFactory ExpiringOpBatch =new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new ExpiredOpBatch();
	}
}; 

public static PoolableObjectFactory UpdatingOpBatch = new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new UpdatingOpBatch();
	}
}; 
public static PoolableObjectFactory ListDomEntryMap =new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new ArrayList<HashMap<ITuple, DomEntry>>();
	}
	
	public void returnObject(Object obj){
		((ArrayList<HashMap<ITuple, DomEntry>>)(obj)).clear();
		super.returnObject(obj);
	}
};


public static PoolableObjectFactory accMap =new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new HashMap<Var, Long>();
	}
};

public static PoolableObjectFactory MuEntry =new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new MappingEntry();
	}
};

public static PoolableObjectFactory arrayFactory(int length){
	if(length==1) return OneArray;
	if(length==3) return ThreeArray;
	return TwoArray;
}

public static PoolableObjectFactory FilterMapping = new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new FilteringMapping();
	}
};



public static PoolableObjectFactory HashSet4MJoinRouter=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new HashSet<MJoinRouter>();
	}
};


public static PoolableObjectFactory UpdatingOp=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new UpdatingOp();
	}
};

public static PoolableObjectFactory ExpiringOp=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new ExpiringOp();
	}
};

public static PoolableObjectFactory PurgingOp=new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new PurgingOp();
	}
};

public static PoolableObjectFactory OpRun =new PoolableObjectFactory() {
	
	@Override
	public Object newObject() {
		return new OpRun();
	}
};
 

public static class OpRun implements Runnable, PoolableObject {
	EventQueue queue;
	
	public OpRun() {}
	
	public void set(EventQueue queue) {
		this.queue = queue;
	}
	@Override
	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseInstance() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void run() {
		queue.deque();
	}
	
}
}
