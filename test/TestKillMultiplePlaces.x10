import harness.x10Test;

import x10.util.resilient.map.*;
import x10.util.Random;
import x10.util.ArrayList;
import x10.util.Timer;
import x10.util.Team;
import x10.util.concurrent.AtomicBoolean;

/**
 * This test is expected to run for long time, and with large number of places (~ >20).
 * Each place runs a long loop in which it increments the value of a randomly selected key from A to Z at each iteration. 
 * To check for correctness, each place records how many times it tried to increment each key. 
 * Place 0 will collect these results, and compare them with the current value of each key.
 * 
 * Test commands: 
 * cd tests
 * make TestKillMultiplePlaces
 * 
 * DS_ALL_VERBOSE=0 X10_NPLACES=6 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestKillMultiplePlaces.o 10 -1
 * 
 * -- run in resilient mode with default parameters 
 * X10_RESILIENT_MODE=1 DS_ALL_VERBOSE=0 X10_NPLACES=6 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestKillMultiplePlaces.o
 * 
 * X10_RESILIENT_MODE=1 DS_ALL_VERBOSE=0 X10_NPLACES=10 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestKillMultiplePlaces.o 100 500
 * 
 * MIGRATION_TIMEOUT_LIMIT=1000 X10_RESILIENT_MODE=1 DS_ALL_VERBOSE=0 X10_NPLACES=10 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestKillMultiplePlaces.o 100 500
 */
public class TestKillMultiplePlaces (maxIterations:Long, killPeriodInMillis:Long) extends x10Test {
    private static KEYS_RAIL = ["A", "B", "C", "D", "E", "F", "G", 
                             "H", "I", "J", "K", "L", "M", "N", 
                             "O", "P", "Q", "R", "S", "T", "U", 
                             "V", "W", "X", "Y", "Z"];
    private static VERBOSE = false;
    private val complete = new AtomicBoolean(false);
    
    val killFlagPLH = PlaceLocalHandle.make[AtomicBoolean](Place.places(), ()=>new AtomicBoolean(false) );	
    
    var killedPlacesStr:String = "";
	public def run(): Boolean {
		
		if (x10.xrx.Runtime.RESILIENT_MODE > 0 && killPeriodInMillis != -1) {
			Console.OUT.println("=========Running in Resilient Mode===========");
			async killPlaces();
		}
		else
			Console.OUT.println("=========Running in NonResilient Mode===========");
		
		val keysCount = KEYS_RAIL.size;		
		val countsPLH = PlaceLocalHandle.make[Rail[Long]](Place.places(), ()=>new Rail[Long](keysCount) );		
		val hm = DataStore.getInstance().makeResilientMap("MapA", 1000);		
		val tmpMaxIterations = maxIterations;
		var valid:Boolean = true;
		try{
			finish for (p in Place.places()) at (p) async {		
				val rnd = new Random(Timer.milliTime()+here.id);
				for (var i:Long = 0 ; i < tmpMaxIterations ; i++) {
					
					if (killFlagPLH().get()) {
						//because this place should die now, I will copy its counts to place 0 for test validation
						val myCounts = countsPLH();						
						at (Place(0)) {
							atomic {
								for (var m:Long = 0; m < keysCount; m++){
									countsPLH()(m) += myCounts(m);
								}
							}
						}
						Console.OUT.println("Killing " + here);						
						System.killHere();
					}
					
					
					val keyIndex = Math.abs(rnd.nextLong()) % keysCount;
					val nextKey = KEYS_RAIL(keyIndex);
					var oldValue:Any;
					var newValue:Any;					
					var r:Long = 0;
				    while(DataStore.getInstance().isValid()) {
				    	if (r == hm.retryMaximum())
				    		Console.OUT.println("Warning: " + here + " has exceeded the max number of retries  r=" + r 
				    							+ " max=" +  hm.retryMaximum() + "  while updating key["+nextKey+"] ");
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
							atomic countsPLH()(keyIndex)++;
							Console.OUT.println(here + "key["+nextKey+"]  Updated from ["+oldValue+"]  to ["+newValue+"] ...");
							break;
						}
						catch (ex:Exception) {
							hm.abortTransactionAndSleep(txId);
						}
					}					
				}
			}			
		}catch(ex2:Exception) {
			ex2.printStackTrace();			
		}
		
		complete.set(true);
		
		if (!DataStore.getInstance().isValid()) {
			return false;
		}
		
		val remainingPlaces = Place.places().filterDeadPlaces();
		val team = new Team(remainingPlaces);
		val sumGR = GlobalRef(new Rail[Long](keysCount)); 
		finish for (p in Place.places()) at (p) async {		
			val src = countsPLH();
			var dst:Rail[Long] = null;
			if (here.id == 0)
				dst = sumGR();
			else
				dst = new Rail[Long](keysCount);
			if (VERBOSE) {
				var srcCountStr:String = "";
				for (var i:Long = 0; i < keysCount; i++) {
					srcCountStr += "[" + KEYS_RAIL(i) + ":" + src(i) + "] ; ";			
				}
				Console.OUT.println( here + "==" + srcCountStr);
			}
			team.allreduce(src, 0, dst, 0, keysCount, Team.ADD);
		}
		
		
		/**Validate at place 0***/
		if (VERBOSE) {
			var dstCountStr:String = "";
			for (var i:Long = 0; i < keysCount; i++) {
				dstCountStr += "[" + KEYS_RAIL(i) + ":" + sumGR()(i) + "] ; ";	
			}
			Console.OUT.println("Total ==" + dstCountStr);
		}
			
		for (var i:Long = 0; i < keysCount; i++) {
			val key = KEYS_RAIL(i);
			val tmpValue = hm.get(key);			
			var foundValue:Long = tmpValue == null? 0 : tmpValue as Long;
			val expectedValue = sumGR()(i);
			if (foundValue != expectedValue) {
				Console.OUT.println("Invalid value for key ["+key+"]   expectedValue["+expectedValue+"]  foundValue["+foundValue+"] ...");
				valid = false;
			}
		}
		
		Console.OUT.println("Killed Places: " + killedPlacesStr);
        return valid;
	}
	
	
	private def killPlaces() {
		var killedPlacesCount:Long = 0;
	
		while (!complete.get() && killedPlacesCount < Place.numPlaces()/2) {
			Console.OUT.println("Place Hammer started ...");
			
			System.sleep(killPeriodInMillis);
			val rnd = new Random(Timer.milliTime()+here.id);
			
			var victim:Long = -1;
			var count:Long = 1;
			do{
				victim = Math.abs(rnd.nextLong()) % Place.numPlaces();
				
				if (victim == 0)
					victim = 1;
				
				if (Place(victim).isDead())
					victim = -1; // try another place
				else 
					break;
				
				count++;
			}while(count < Place.numPlaces());
			
			if (victim != -1){	
				killedPlacesCount++;
				killedPlacesStr += victim + ",";
				at (Place(victim)) {
					killFlagPLH().set(true);
				}
			}
		}
	}

	/*
	 * args: <optional_place_to_kill> 
	 **/
	public static def main(var args: Rail[String]): void {
		if (Place.numPlaces() < 3) {
			Console.OUT.println("ResilientMap tests require at least 3 places to be used");
		    return;	
		}
		
		var maxIteration:Long = Long.MAX_VALUE;
		
		var killPeriodInMillis:Long = -1;
		if (x10.xrx.Runtime.RESILIENT_MODE > 0)
			killPeriodInMillis = 100;
		
		if (args.size == 2) {
			maxIteration = Long.parseLong(args(0));
			killPeriodInMillis = Long.parseLong(args(1));
		}
		Console.OUT.println("Starting [TestKillMultiplePlaces] with maxIterations = " + maxIteration  + " killPeriod = " + killPeriodInMillis);
		new TestKillMultiplePlaces(maxIteration, killPeriodInMillis).execute();
	}
}
