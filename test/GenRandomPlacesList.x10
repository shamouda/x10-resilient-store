import x10.util.Random;
import x10.util.ArrayList;
import x10.util.Timer;

public class GenRandomPlacesList {	
	private static def findVictimPlaces(placesCount:Long, iterationsCount:Long) {
	    val rnd = new Random(Timer.milliTime()+here.id);
	    val list = new ArrayList[Long]();
	    val iters = new ArrayList[Long]();
	    
	    for (var c:Long = 0; c < placesCount/4; c++) {
	        var victim:Long = Math.abs(rnd.nextLong() %placesCount);
			if (victim == 0)
			    victim = 1;
		
			if (list.contains(victim)) {
			    c--;
			}
			else {
			    val iter = Math.abs(rnd.nextLong() %iterationsCount);
			    list.add(victim);
			    iters.add(iter);
			}
		}
	    var str:String = "Places:";
	    for (x in list) {
	        str += x + ",";
	    }
	    Console.OUT.println(str);
	    
	    var strIter:String = "Iterations:";
        for (x in iters) {
            strIter += x + ",";
        }
        Console.OUT.println(strIter);
        
        
	}

	public static def main(var args: Rail[String]): void {
	    findVictimPlaces(250, 500);
	}
}




