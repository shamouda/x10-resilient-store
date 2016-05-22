import harness.x10Test;

import x10.util.resilient.map.*;
import x10.util.Random;
import x10.util.ArrayList;

/**
 * Test case where each place is required to increment all keys in a ResilientMap,
 * but the order of the keys is determined randomly at each place
 * 
 * 
 * Test commands: 
 * cd tests; make 
 * DS_ALL_VERBOSE=0 X10_NPLACES=10 FORCE_ONE_PLACE_PER_NODE=1 ./default.o
 * 
 * -- kill place 3:
 * X10_RESILIENT_MODE=1 DS_ALL_VERBOSE=0 X10_NPLACES=10 FORCE_ONE_PLACE_PER_NODE=1 ./default.o 3
 */
public class TestAllPlacesIncrementAllKeys(placeToKill:Long) extends x10Test {
    private static KEYS_RAIL = ["A", "B", "C", "D", "E", "F", "G", 
                             "H", "I", "J", "K", "L", "M", "N", 
                             "O", "P", "Q", "R", "S", "T", "U", 
                             "V", "W", "X", "Y", "Z"];
    
	public def run(): Boolean {
		val hm = DataStore.getInstance().makeResilientMap("MapA", 100);
		var valid:Boolean = true;
		try{
			finish for (p in Place.places()) at (p) async {
				val keyIndexList = new ArrayList[Long]();
				for (i in 0..(KEYS_RAIL.size-1))
					keyIndexList.add(i);
				
				val rnd = new Random(here.id);              
				val keysCount = KEYS_RAIL.size;
				
				var killIteration:Long = -1;
				
				if (here.id == placeToKill){
					killIteration = Math.abs(rnd.nextLong()) % keyIndexList.size();
				}
				for (var i:Long = 0 ; i < keysCount ; i++) {
					
					if (i == killIteration) {
						async System.killHere();
					}

					val index = Math.abs(rnd.nextLong()) % keyIndexList.size();
					val keyIndex = keyIndexList.get(index);
					val nextKey = KEYS_RAIL(keyIndex);
					for (var r:Long = 0 ; r < hm.retryMaximum(); r++) {
						val txId = hm.startTransaction();
						try{
							val x = hm.get(txId, nextKey);
							if (x == null) {
								hm.put(txId, nextKey, 1);
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
				Console.OUT.println("Invalid value for key ["+key+"]   expectedValue["+Place.numPlaces()+"]  foundValue["+foundValue+"] ...");
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
