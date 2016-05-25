package x10.util.resilient.map.migration;

import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;
import x10.util.resilient.map.common.Utils;

public class MigrationRequest (partitionId:Long, oldReplicas:HashSet[Long], newReplicas:HashSet[Long]) {
    private val moduleName = "MigrationHandler";
    public static val VERBOSE = Utils.getEnvLong("MIG_MNGR_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    
    private var completed:Boolean = false;
    private var success:Boolean = false;
    private var startTimeMillis:Long = -1;

    private val lock:SimpleLatch = new SimpleLatch();
    
    public def start() {
        startTimeMillis = Timer.milliTime();
        if (VERBOSE) Utils.console(moduleName, "migration request for partitionId["+partitionId+"] started at ["+startTimeMillis+"]");
    }
    
    public def complete(success:Boolean) {
    	lock.lock();    	
    	if (!completed) {
    		if (VERBOSE) Utils.console(moduleName, "migration request for partitionId["+partitionId+"] is complete");
    		completed = true;
    		this.success = success;
    	}
    	else
    		if (VERBOSE) Utils.console(moduleName, "IGNORING migration response, request is complete");
        lock.unlock();
    }

    public def isSuccessful() : Boolean {
    	lock.lock();
    	val result = completed && success;
    	lock.unlock();
    	return result;
    }
    public def isComplete() : Boolean {
    	lock.lock();
    	val result = completed;
    	lock.unlock();
    	return result;
    }
    public def isTimeOut(limit:Long):Boolean {
        val curTime = Timer.milliTime();
        val elapsedTime = curTime - startTimeMillis;
        if (VERBOSE) Utils.console(moduleName, "Timeout limit ["+limit+"]  startTime["+startTimeMillis+"]  elapsedTime["+elapsedTime+"]   result ["+(elapsedTime > limit)+"] ...");
        return elapsedTime > limit;
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