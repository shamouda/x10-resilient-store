package x10.util.resilient.map.impl.migration;

import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;

public class MigrationRequest (partitionId:Long, oldReplicas:HashSet[Long], newReplicas:HashSet[Long]) {
    private var completed:Boolean = false;
    private var startTimeMillis:Long = -1;

    public def start() {
        startTimeMillis = Timer.milliTime();
    }
    
    public def complete() {
        completed = true;
    }

    public def isComplete() = completed;
    public def isTimeOut(limit:Long) = (Timer.milliTime()-startTimeMillis) > limit;
    
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