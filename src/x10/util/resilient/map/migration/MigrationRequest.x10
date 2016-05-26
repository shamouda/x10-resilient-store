package x10.util.resilient.map.migration;

import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;
import x10.util.resilient.map.common.Utils;

public class MigrationRequest (partitionId:Long, oldReplicas:HashSet[Long], newReplicas:HashSet[Long]) {
    private val moduleName = "MigrationHandler";
    public static val VERBOSE = Utils.getEnvLong("MIG_MNGR_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    
    private var completed:Boolean = false;
    private var startTimeMillis:Long = -1;

    public def start() {
        startTimeMillis = Timer.milliTime();
        if (VERBOSE) Utils.console(moduleName, "migration request for partitionId["+partitionId+"] started at ["+startTimeMillis+"]");
    }
    
    public def complete() {
        if (VERBOSE) Utils.console(moduleName, "migration request for partitionId["+partitionId+"] is complete");
        completed = true;
    }

    public def isComplete() = completed;
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