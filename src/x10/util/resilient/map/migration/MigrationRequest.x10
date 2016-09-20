package x10.util.resilient.map.migration;

import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;

public class MigrationRequest (partitionId:Long, oldReplicas:HashSet[Long], newReplicas:HashSet[Long]) {
    private val moduleName = "MigrationHandler";
    private var completed:Boolean = false;
    private var startTimeMillis:Long = -1;

    public def start() {
        startTimeMillis = Timer.milliTime();
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "migration request for partitionId["+partitionId+"] started at ["+startTimeMillis+"]"); }
    }
    
    public def complete() {
    	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "migration request for partitionId["+partitionId+"] is complete"); }
        completed = true;
    }

    public def isComplete() = completed;
    public def isTimeOut(limit:Long):Boolean {
        val curTime = Timer.milliTime();
        val elapsedTime = curTime - startTimeMillis;
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Timeout limit ["+limit+"]  startTime["+startTimeMillis+"]  elapsedTime["+elapsedTime+"]   result ["+(elapsedTime > limit)+"] ..."); }
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