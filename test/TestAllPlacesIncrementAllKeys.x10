import harness.x10Test;

import x10.util.resilient.map.*;
import x10.util.Random;
import x10.util.ArrayList;
import x10.util.Timer;

/**
 * Test case where each place is required to increment all keys in a ResilientMap,
 * but the order of the keys is determined randomly at each place
 * 
 * 
 * Test commands: 
 * cd tests
 * make TestAllPlacesIncrementAllKeys
 * 
 * --without killing places
 * X10_NPLACES=10 FORCE_ONE_PLACE_PER_NODE=1 ./TestAllPlacesIncrementAllKeys.o
 * 
 * -- kill place 3:  (Place 3 is: Replica and ReplicaClient )
 * X10_RESILIENT_MODE=1 X10_NPLACES=10 FORCE_ONE_PLACE_PER_NODE=1 ./TestAllPlacesIncrementAllKeys.o 3
 * 
 * -- kill place 3:  (Place 3 is a Leader)
 * X10_RESILIENT_MODE=1 X10_NPLACES=4 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestAllPlacesIncrementAllKeys.o 3
 * 
 * -- kill place 4 (Place 4 is Deputy Leader)
 * X10_RESILIENT_MODE=1 X10_NPLACES=6 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestAllPlacesIncrementAllKeys.o 4
 */
public class TestAllPlacesIncrementAllKeys(placeToKill:Long) extends x10Test {
    private static KEYS_RAIL = ["A", "B", "C", "D", "E", "F", "G", 
                             "H", "I", "J", "K", "L", "M", "N", 
                             "O", "P", "Q", "R", "S", "T", "U", 
                             "V", "W", "X", "Y", "Z"];
    
	public def run(): Boolean {
		val hm = DataStore.getInstance().makeResilientMap("MapA");
		var valid:Boolean = true;
		try{
			finish for (p in Place.places()) at (p) async {
				val keyIndexList = new ArrayList[Long]();
				for (i in 0..(KEYS_RAIL.size-1))
					keyIndexList.add(i);
				
				val rnd = new Random(Timer.milliTime()+here.id);              
				val keysCount = KEYS_RAIL.size;
				
				if (here.id == placeToKill){
				    Console.OUT.println("Killing " + here);
                    System.killHere();
				}
				
				for (var i:Long = 0 ; i < keysCount ; i++) {
					val index = Math.abs(rnd.nextLong()) % keyIndexList.size();
					val keyIndex = keyIndexList.get(index);
					val nextKey = KEYS_RAIL(keyIndex);
					var keySuccess:Boolean = false;
					var oldValue:Any;
					var newValue:Any;
					
					for (var r:Long = 0 ; r < hm.retryMaximum(); r++) {
						val txId = hm.startTransaction();
						try{
							val x = hm.get(txId, nextKey);
							oldValue = x;
							if (x == null) {
								hm.put(txId, nextKey, 1);
								newValue = 1;
							}
							else {
								hm.put(txId, nextKey, (x as Long)+1);
								newValue = (x as Long)+1;
							}
							hm.commitTransaction(txId);
							Console.OUT.println(here + "key["+nextKey+"]  Updated from ["+oldValue+"]  to ["+newValue+"] ...");
							keySuccess = true;
							break;
						}
						catch (ex:Exception) {
							hm.abortTransactionAndSleep(txId);
						}
					}
					if (!keySuccess)
						Console.OUT.println(here + "key["+nextKey+"]  all retries failed ...");
					keyIndexList.removeAt(index);
				}
			}			
		}catch(ex2:Exception) {
			ex2.printStackTrace();			
		}
		
		
		/**Validate at place 0***/
		val expectedValue = (placeToKill == -1) ? Place.numPlaces() : Place.numPlaces()-1 ;
		for (key in KEYS_RAIL) {
			val foundValue = hm.get(key) as Long;
			if (foundValue != expectedValue) {
				Console.OUT.println("Invalid value for key ["+key+"]   expectedValue["+expectedValue+"]  foundValue["+foundValue+"] ...");
				valid = false;
			}
		}
		
		
        return valid;
	}

	/*
	 * args: <optional_place_to_kill> 
	 **/
	public static def main(var args: Rail[String]): void {
		if (Place.numPlaces() < 3) {
			Console.OUT.println("ResilientMap tests require at least 3 places to be used");
		    return;	
		}
		var placeToKill:Long = -1;
		if (args.size > 0)
			placeToKill = Long.parseLong(args(0));
		
		new TestAllPlacesIncrementAllKeys(placeToKill).execute();
	}
}
