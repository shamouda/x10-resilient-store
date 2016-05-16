package x10.util.resilient.map.migration;

import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;

public class MigrationRequest (partitionId:Long, oldReplicas:HashSet[Long], newReplicas:HashSet[Long]) {
	private val lock:SimpleLatch = new SimpleLatch();
    private var startTimeMillis:Long = -1;

    public def wait() {
    	startTimeMillis = Timer.milliTime();
    	lock.await();
    }
    
    public def complete() {
    	lock.release();
    }

	public def toString():String {
		var str:String = "partitionId:"+partitionId + "=> old[";
	    for (x in oldReplicas)
	    	str += x + ",";
		str += "]  new[";
	    for (y in newReplicas)
	     	str += y + ",";
		str += "] ";
		return str;
	}
}