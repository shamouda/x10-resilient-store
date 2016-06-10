import harness.x10Test;

import x10.util.resilient.map.*;
import x10.util.Random;
import x10.util.ArrayList;
import x10.util.Timer;
import x10.util.Team;
import x10.util.concurrent.AtomicBoolean;
import x10.util.HashMap;

import x10.util.Option;
import x10.util.OptionsParser;

/**
 * This test is expected to run for a long time, and with large number of places (~ >20).
 * Each place runs a long loop in which it increments the value of a randomly selected key from A to Z at each iteration. 
 * To check for correctness, each place records how many times it tried to increment each key. 
 * Place 0 will collect these results, and compare them with the current value of each key.
 * 
 * Test commands: 
 * cd tests
 * make TestKillMultiplePlaces
 * 
 * DATA_STORE_LEADER_NODE=3
 * FORCE_ONE_PLACE_PER_NODE=1
 * DS_ALL_VERBOSE=0
 * KILL_WHILE_COMMIT_PLACE_ID=1 
 * KILL_WHILE_COMMIT_TRANS_COUNT=3 
 * MIGRATION_TIMEOUT=1000 
 * 
 * X10_RESILIENT_MODE=0 DS_ALL_VERBOSE=0 X10_NPLACES=6 FORCE_ONE_PLACE_PER_NODE=1 DATA_STORE_LEADER_NODE=3 ./TestKillMultiplePlaces.o -m 40 -c 1 -q 100 -vp 2,5 -vi 5,25
 */
public class TestKillMultiplePlaces (verify:Boolean, maxIterations:Long, killPeriodInMillis:Long, killedPlacesPercentage:Float, enableConflict:Boolean, victimPlaces:String, victimIterations:String) extends x10Test {
    private static KEYS_RAIL = ["A", "B", "C", "D", "E", "F", "G", 
                             "H", "I", "J", "K", "L", "M", "N", 
                             "O", "P", "Q", "R", "S", "T", "U", 
                             "V", "W", "X", "Y", "Z"];
    private static VERBOSE = false;
    private val complete = new AtomicBoolean(false);
    
    val killFlagPLH = PlaceLocalHandle.make[AtomicBoolean](Place.places(), ()=>new AtomicBoolean(false) );	
    
	public def run(): Boolean {
		if (x10.xrx.Runtime.RESILIENT_MODE > 0 && killPeriodInMillis != -1 && victimIterations.equals("")) {
			Console.OUT.println("=========Starting random iteration place hammer===========");
			async killPlaces();
		}
		
		val startTime = Timer.milliTime();
		val keysCount = KEYS_RAIL.size;
		val localStatePLH = PlaceLocalHandle.make[LocalState](Place.places(), ()=>new LocalState() );		
		val localStatesOfDeadPlacesGR = GlobalRef(new ArrayList[LocalState]());
		val hm = DataStore.getInstance().makeResilientMap("MapA");
		////////////////warming up//////////////////////////////////
		finish for (p in Place.places()) at (p) async {
			DataStore.getInstance();
		}
		//////////////////////////////////////////////////////////
		
		val tmpMaxIterations = maxIterations;
		var valid:Boolean = true;
		val map = createVictimMap(victimPlaces, victimIterations);
		try{
			finish for (p in Place.places()) at (p) async {
				val rnd = new Random(Timer.milliTime()+here.id);
				for (var i:Long = 0 ; i < tmpMaxIterations ; i++) {
					localStatePLH().totalIterations = i;
					
					val killIteration = map.getOrElse(here.id, -1);
					if (killFlagPLH().get() || killIteration == i) {
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
							val currentCount = localStatePLH().keyUpdateCount.getOrElse(nextKey, 0);
							localStatePLH().keyUpdateCount.put(nextKey, currentCount+1);							
//							Console.OUT.println(here + "key["+nextKey+"]  Updated from ["+oldValue+"]  to ["+newValue+"] tx["+txId+"] ...");
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
			//ex2.printStackTrace();			
		}
		
		val elapsedTime = Timer.milliTime() - startTime;
		if (!hm.isValid())
			return false;
		
		complete.set(true);
		
		
		/**Validate at place 0***/
		val sumKeysCount = new HashMap[String,Long]();
		var maxIterations:Long = -1;
		var placeKillIter:String = "";
		for (localState in localStatesOfDeadPlacesGR()){
			//Console.OUT.println(localState.toString());
			val iter = localState.keyUpdateCount.keySet().iterator();
			while (iter.hasNext()) {
			    val key = iter.next();
			    val count = localState.keyUpdateCount.getOrElse(key, 0);
			    val sum = sumKeysCount.getOrElse(key, 0);
			    sumKeysCount.put(key, sum + count);
			}
			
			if (maxIterations < localState.totalIterations)
				maxIterations = localState.totalIterations;
			
			if (localState.killedAtIteration != -1) {
			    placeKillIter += localState.placeId + ":" + localState.killedAtIteration + ";" ;
			}
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
	
		if (verify) {
		    val iter = sumKeysCount.keySet().iterator();
		    while (iter.hasNext()) {
		        val key = iter.next();
		        val tmpValue = hm.get(key);
		        var foundValue:Long = tmpValue == null? 0 : tmpValue as Long;
		        val expectedValue = sumKeysCount.getOrElse(key, 0);
		        if (foundValue != expectedValue) {
		            Console.OUT.println("Invalid value for key ["+key+"]   expectedValue["+expectedValue+"]  foundValue["+foundValue+"] ...");
		            valid = false;
		        }
		    }
		}
		Console.OUT.println("Place Kill Iter: " + placeKillIter);
		Console.OUT.println("Elapsed Time (ms):" + elapsedTime);
        return valid;
	}
	
	private def killPlaces() {
		var killedPlacesCount:Long = 0;
	    
	    val victimsList = new ArrayList[Long]();
	    for (x in victimPlaces.split(","))
	        victimsList.add(Long.parseLong(x));
	    var victimIndex:Long = 0;
		while (!complete.get() ) {
		    if ( victimsList.size() > 0 && victimIndex == victimsList.size() || 
		         (killedPlacesCount as Float)/Place.numPlaces() >= killedPlacesPercentage ) {
		        break;
		    }
			Console.OUT.println("Place Hammer started ...");
			
			System.sleep(killPeriodInMillis);
			val rnd = new Random(Timer.milliTime()+here.id);
			
			var victim:Long = -1;
			var count:Long = 1;
			do{
			    if (victimsList.size() > 0)
			        victim = victimsList.get(victimIndex);
			    else {
			        victim = Math.abs(rnd.nextLong()) % Place.numPlaces();
			        if (victim == 0)
			            victim = 1;
			    }
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
			    victimIndex++;
				killedPlacesCount++;
				at (Place(victim)) {
					killFlagPLH().set(true);
				}
			}
		}
	}
	
	public def createVictimMap(victimPlaces:String, victimIterations:String):HashMap[Long,Long] {
	    if (x10.xrx.Runtime.RESILIENT_MODE == 0n || victimPlaces == null || victimPlaces.equals(""))
	        return new HashMap[Long,Long]();
	    val placesRail = victimPlaces.split(",");
	    val victimsRail = victimIterations.split(",");
	    val map = new HashMap[Long,Long]();
	    for (var i:Long = 0; i < placesRail.size; i++) {
	        map.put(Long.parseLong(placesRail(i)), Long.parseLong(victimsRail(i)));
	    }
	    return map;
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
        val opts = new OptionsParser(args, [
            Option("h","help","this information"),
            Option("v","verify","verify the results")
        ], [
            Option("m","maxIterations","iterations count per place"),
            Option("c","conflit","enable conflicts between places"),
            Option("p","killPercentage","Percentage of killed places"),      
            Option("q","killPeriod","Time between killing places"),
            Option("vp","victimPlaces","Places to kill"),
            Option("vi","victimIterations","Iterations to kill at")
        ]);
		
        val verify:Boolean = opts("v", false);
		val maxIteration:Long = opts("m", Long.MAX_VALUE);
        val enableConflict:Boolean = opts("c", true);
		val killedPlacesPercentage:Float = opts("p", 0.5F);
		val killPeriodInMillis:Long = opts("q", 100);
        val victimPlaces = opts("vp", "");
        val victimIterations = opts("vi", "");
        
		Console.OUT.println("Starting [TestKillMultiplePlaces]   maxIterations=" + maxIteration  + " killPeriod=" + killPeriodInMillis + " killedPercentage=" + killedPlacesPercentage + " enableConflict="+enableConflict);
		new TestKillMultiplePlaces(verify, maxIteration, killPeriodInMillis, killedPlacesPercentage, enableConflict, victimPlaces, victimIterations).execute();
	}
}



class LocalState {
	val placeId = here.id;
	val keyUpdateCount:HashMap[String,Long] = new HashMap[String,Long]();
    var totalIterations:Long;
    val iterationTime = new ArrayList[Long]();
    var killedAtIteration:Long;
	
	public def toString():String {
		var str:String = "Place("+placeId+"): totalIters["+totalIterations+"] killedAtIter["+killedAtIteration+"] ";
	    str += "IterationTime[" ;
	    for (x in iterationTime)
	    	str += x + ",";
	    str += "]  ";/*keyUpdateCount[";
	    for (x in keyUpdateCount)
	    	str += x + ",";
	    str += "]";*/
		return str;
	}
}
