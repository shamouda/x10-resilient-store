import harness.x10Test;

import x10.util.resilient.map.*;
import x10.util.Random;
import x10.util.ArrayList;

/**
 * Test case where each place stores a big local array, then retrieves it
 */
public class TestAllPlacesStoreLocalData(dummyRailSize:Long) extends x10Test {
    private static RETRY_COUNT = Place.numPlaces();
    
    
	public def run(): Boolean {
		val hm = DataStore.getInstance().makeResilientMap("MapA");
		finish for (p in Place.places()) at (p) async {			 
			val dummyRail = makeDummyRail();
		}
        return true;
	}
	
	private static makeDummyRail():Rail[Double] {
		
		val rail = new Rail[Double]();
	}

	/*
	 * args: array_size <optional_place_to_kill> 
	 **/
	public static def main(var args: Rail[String]): void {
		var size:Long = 1024;
		if (args.size > 0) {
			size = Long.parseLong(args(0));
		}
		new TestAllPlacesStoreLocalData(size).execute();
	}
}
