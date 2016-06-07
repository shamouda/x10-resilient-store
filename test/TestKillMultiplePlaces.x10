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
 * 
 * KILL_WHILE_COMMIT_PLACE_ID=1 KILL_WHILE_COMMIT_TRANS_COUNT=3 X10_RESILIENT_MODE=1 DS_ALL_VERBOSE=1 X10_NPLACES=10 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestKillMultiplePlaces.o 100 500
 * 
 * --no conflict
 * MIGRATION_TIMEOUT_LIMIT=1000 X10_RESILIENT_MODE=1 DS_ALL_VERBOSE=0 X10_NPLACES=10 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestKillMultiplePlaces.o 100 500
 */
public class TestKillMultiplePlaces (maxIterations:Long, killPeriodInMillis:Long, killedPlacesPercentage:Float, enableConflict:Boolean) extends x10Test {
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
		val localStatePLH = PlaceLocalHandle.make[LocalState](Place.places(), ()=>new LocalState(keysCount) );		
		val localStatesOfDeadPlacesGR = GlobalRef(new ArrayList[LocalState]());
		
		val hm = DataStore.getInstance().makeResilientMap("MapA", 1000);		
		val tmpMaxIterations = maxIterations;
		var valid:Boolean = true;
		try{
			finish for (p in Place.places()) at (p) async {
				val rnd = new Random(Timer.milliTime()+here.id);
				for (var i:Long = 0 ; i < tmpMaxIterations ; i++) {
					localStatePLH().totalIterations = i;
					
					if (killFlagPLH().get()) {
						//because this place should die now, I will copy its counts to place 0 for test validation
						val myLocalState = localStatePLH();
						myLocalState.killedAtIteration = i;
						at (Place(0)) {
							atomic localStatesOfDeadPlacesGR().add(myLocalState);
						}
						Console.OUT.println("Killing " + here);						
						System.killHere();
					}
					
					val keyIndex = Math.abs(rnd.nextLong()) % keysCount;
					var tmpKey:String = KEYS_RAIL(keyIndex);
					if (!enableConflict) //append place id to key to avoid conflict between places
						tmpKey += here.id;
					val nextKey = tmpKey;
					
					var oldValue:Any;
					var newValue:Any;					
					var r:Long = 0;
					val startIteration = Timer.milliTime();
				    while(hm.isValid()) {
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
							
							localStatePLH().iterationTime.add(Timer.milliTime()-startIteration);
							localStatePLH().keyUpdateCount(keyIndex)++;							
							Console.OUT.println(here + "key["+nextKey+"]  Updated from ["+oldValue+"]  to ["+newValue+"] tx["+txId+"] ...");
							break;
						}
						catch (ex:Exception) {
							hm.abortTransactionAndSleep(txId);
						}
					}					
				}
				val myLocalState = localStatePLH();
				myLocalState.killedAtIteration = -1;
				myLocalState.totalIterations++;
				at (Place(0)) {
					atomic localStatesOfDeadPlacesGR().add(myLocalState);
				}
			}			
		}catch(ex2:Exception) {
			ex2.printStackTrace();			
		}
		
		if (!hm.isValid())
			return false;
		
		complete.set(true);
		
		
		/**Validate at place 0***/
		val sumKeysCount = new Rail[Long](keysCount);
		var maxIterations:Long = -1;
		for (localState in localStatesOfDeadPlacesGR()){
			Console.OUT.println(localState.toString());
			for (var i:Long = 0; i < keysCount; i++) {
				sumKeysCount(i) += localState.keyUpdateCount(i);
			}
			if (maxIterations < localState.totalIterations)
				maxIterations = localState.totalIterations;
		}
		
		val sumTimePerIteration = new Rail[Long](maxIterations);
		val sumCountPerIteration = new Rail[Long](maxIterations);
		val averageTimePerIteration = new Rail[Float](maxIterations);
		for (localState in localStatesOfDeadPlacesGR()){
			for (var i:Long = 0; i < localState.totalIterations; i++) {
				sumTimePerIteration(i) += localState.iterationTime.get(i);
				sumCountPerIteration(i)++;
			}
		}
		
		var avgStr:String = "";
		for (var i:Long = 0; i < maxIterations; i++) {
			averageTimePerIteration(i) = ((sumTimePerIteration(i) as Float)/sumCountPerIteration(i));
			avgStr += Math.round(averageTimePerIteration(i))  + ",";
		}
		
		Console.OUT.println("AverageTime: " + avgStr);
	
		for (var i:Long = 0; i < keysCount; i++) {
			val key = KEYS_RAIL(i);
			val tmpValue = hm.get(key);
			var foundValue:Long = tmpValue == null? 0 : tmpValue as Long;
			val expectedValue = sumKeysCount(i);
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
	
		while (!complete.get() && (killedPlacesCount as Float)/Place.numPlaces() < killedPlacesPercentage ) {
			Console.OUT.println("Place Hammer started  reached("+(killedPlacesCount as Float)/Place.numPlaces()+") ...");
			
			System.sleep(killPeriodInMillis);
			val rnd = new Random(Timer.milliTime()+here.id);
			
			var victim:Long = -1;
			var count:Long = 1;
			do{
				victim = Math.abs(rnd.nextLong()) % Place.numPlaces();
				
				if (victim == 0)
					victim = 1;
				
				var consideredDead:Boolean = false;
				try{
					consideredDead = at (Place(victim)) killFlagPLH().get();
				}catch(ex:Exception) {
					consideredDead = true;
				}
				
				if (consideredDead)
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

	//TODO: for statistical analysis, we need to be able to repeat the same scenario multiple times
	/*
	 * args: <optional_place_to_kill>  <kill_period> <percentage_of_killed_places> <enable_conflict>
	 **/
	public static def main(var args: Rail[String]): void {
		if (Place.numPlaces() < 3) {
			Console.OUT.println("ResilientMap tests require at least 3 places to be used");
		    return;	
		}
		
		var maxIteration:Long = Long.MAX_VALUE;		
		var killedPlacesPercentage:Float = 0.5F;
		var enableConflict:Boolean = true;
		
		var killPeriodInMillis:Long = -1;
		if (x10.xrx.Runtime.RESILIENT_MODE > 0)
			killPeriodInMillis = 100;
		
		if (args.size == 2) {
			maxIteration = Long.parseLong(args(0));
			killPeriodInMillis = Long.parseLong(args(1));
		}
		else if (args.size == 3) {
			maxIteration = Long.parseLong(args(0));
			killPeriodInMillis = Long.parseLong(args(1));
			killedPlacesPercentage = Float.parseFloat(args(2));
		}
		else if (args.size == 4) {
			maxIteration = Long.parseLong(args(0));
			killPeriodInMillis = Long.parseLong(args(1));
			killedPlacesPercentage = Float.parseFloat(args(2));
			enableConflict = Long.parseLong(args(3)) == 1;
		}
		
		Console.OUT.println("Starting [TestKillMultiplePlaces] with maxIterations = " + maxIteration  + " killPeriod = " + killPeriodInMillis);
		new TestKillMultiplePlaces(maxIteration, killPeriodInMillis, killedPlacesPercentage, enableConflict).execute();
	}
}


class LocalState {
	val placeId:Long;
	val keyUpdateCount:Rail[Long];
    var totalIterations:Long;
    val iterationTime = new ArrayList[Long]();
    var killedAtIteration:Long;
    
	public def this(keysCount:Long) {
		keyUpdateCount = new Rail[Long](keysCount);
		placeId = here.id;
	}
	
	public def toString():String {
		var str:String = "Place("+placeId+"): totalIters["+totalIterations+"] killedAtIter["+killedAtIteration+"] ";
	    str += "IterationTime[" ;
	    for (x in iterationTime)
	    	str += x + ",";
	    str += "]  keyUpdateCount[";
	    for (x in keyUpdateCount)
	    	str += x + ",";
	    str += "]";
		return str;
	}
}
