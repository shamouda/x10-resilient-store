import harness.x10Test;

import x10.util.resilient.map.*;
import x10.util.Random;
import x10.util.ArrayList;

/**
 * Test case where each place is required to increment all keys in a ResilientMap,
 * but the order of the keys is determined randomly at each place
 */
public class TestAllPlacesIncrementAllKeys extends x10Test {
    private static RETRY_COUNT = Place.numPlaces();
    private static KEYS_RAIL = ["A", "B", "C", "D", "E", "F", "G", 
                             "H", "I", "J", "K", "L", "M", "N", 
                             "O", "P", "Q", "R", "S", "T", "U", 
                             "V", "W", "X", "Y", "Z"];
    
	public def run(): Boolean {
		val hm = DataStore.getInstance().makeResilientMap("MapA", 100);
		finish for (p in Place.places()) at (p) async {			 
			val keyIndexList = new ArrayList[Long]();
			for (i in 0..(KEYS_RAIL.size-1))
				keyIndexList.add(i);
			
			val rnd = new Random(here.id);              
			val keysCount = KEYS_RAIL.size;
			do {
				val index = rnd.nextLong() % keyIndexList.size();
				val nextKey = KEYS_RAIL(index);
				
				
				for (var r:Long = 0 ; r < RETRY_COUNT; r++) {
	                val txId = hm.startTransaction();
	                try{
	                    val x = hm.get(txId, nextKey);
	                    if (x == null) {	                        
	                        hm.put(txId, nextKey, 0);
	                    }
	                    else {	                        
	                        hm.put(txId, nextKey, (x as Long)+1);
	                    }
	            
	                    hm.commitTransaction(txId);
	                    break;
	                }
	                catch (ex:Exception) {
	                    hm.abortTransaction(txId);
	                }
	            }
				
				keyIndexList.remove(index);
				
			}
			while(keyIndexList.size() > 0);
			 
		}
        return true;
	}

	public static def main(var args: Rail[String]): void {
		new TestAllPlacesIncrementAllKeys().execute();
	}
}
