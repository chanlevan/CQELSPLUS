package cqelsplus.execplan.data;

//import it.unimi.dsi.fastutil.HashCommon;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import cqelsplus.execplan.oprouters.IStatefulRouter;
import cqelsplus.execplan.utils.HashCommon;

public class ToBeExpiredBuff extends BatchBuff {
	Logger logger = Logger.getLogger(ToBeExpiredBuff.class);
	IStatefulRouter sop;
	
	/**To avoid iterate over the whole saved expiration buffer,
	 *we should save the number of leaf mapping contained.
	 */
	
	HashMap<Integer, DomEntry> leafHash;
	public ToBeExpiredBuff(IStatefulRouter sop) {
		super();
		this.sop = sop;
		this.leafHash = new HashMap<Integer, DomEntry>();
	}
	/**
	 * This method is used to consider a saved item if it contains an expired mapping and purge the value if it is 
	 */
	public void purge(Cont_Dep_ExpM expLeaf) {
		
		boolean stopLooping = false;

		MappingEntry cur = head;
		while (cur != null) {
			if (cur.getElm().contains(expLeaf)) {
				ArrayList<ITuple> leaves = cur.getElm().getLeaveTuples();
				/**if the current element contains this leaf then all the leaf hashes
				 * related to this leaf has to be decrease 1 item*/
				for (int leafPos = 0; leafPos < leaves.size(); leafPos++) {
					/**consider hash table for item corresponding to this position*/
					/**consider the leaf mapping in this position*/
					LeafTuple otherLeaf = (LeafTuple)leaves.get(leafPos);
					/**apply murmurhash here with the MUN*/
					int hashKey = getHashCode(otherLeaf, leafPos);
					DomEntry entry = leafHash.get(hashKey);
					if (entry == null) {
						logger.warn("Can't find entry with corresponding key");
						return;
					}
					try {
						entry.decrCount();
						if ( entry.count() == 0) {
							/**this is the point to know we should stop iterating
							 *The other leaf is the expired leaf if it is not only the same by the time stamp
							 *but also the same by the position of virtual window. This can help avoiding the self join problem
							 */
							synchronized (leafHash) {
								leafHash.remove(hashKey);
							}
							if (expLeaf.getLeafTuple().timestamp == (otherLeaf.timestamp) && 
									cur.getElm().getProbingSequence().getVWPosition(expLeaf.getVW()) == leafPos) {
								stopLooping = true;
							} 
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				sop.expireOne(cur.getElm());
				
				MappingEntry tmp = cur.next;
				this.remove(cur);
				cur = tmp;
				
				if (stopLooping) {
					return;
				}

			} else {/**the current consideration is not containing the leaf*/
				cur = cur.next;
			}
		}
	}
	/**muPos or leafPos, in this case, are the same */
	private int getHashCode(LeafTuple mun, int vwPos) {
		return 31*mun.hashCode() + HashCommon.murmurHash3(vwPos);
	}
	
	@Override
	public void addBuff(BatchBuff batch) {
		/**Append the new buffer to the existing one*/
		super.addBuff(batch);
		/**Iterate over each element*/
		MappingEntry cur = head;
		while (cur != null) {
			/**Get the leaf mapping from the current element*/
			ArrayList<ITuple> leaves = cur.getElm().getLeaveTuples();
			for (int leafPos = 0; leafPos < leaves.size(); leafPos++) {
				LeafTuple leaf = (LeafTuple)leaves.get(leafPos);
				/**Save the number of leaf mapping identified by all of its values and position of it in the 
				 *intermediate mapping. We need to do this because when any of these leaf mappings expire it leads to 
				 *the accumulated value. Furthermore, we need to save the number of leaf mappings apprearing so that 
				 *we can have a chance to stop in advanced when looping through the expired buffer.*/
				int hashKey = getHashCode(leaf, leafPos);
				DomEntry de = leafHash.get(hashKey);
				if (de == null) {
					de = (DomEntry)POOL.DomEntry.borrowObject();
					de.setCount(1);
				} else {
					de.incCount();
				}
				synchronized(leafHash) {
					leafHash.put(hashKey, de);
				}
			}
			cur = cur.next;
		}
	}
}
 