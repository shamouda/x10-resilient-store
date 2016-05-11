package x10.util.resilient.map.partition;

import x10.util.HashSet;

public class PartitionReplicas (partitionId:Long, replicas:HashSet[Long]) {
    
    public def toString():String {
        var str:String = "";
        str += "ParitionId: " + partitionId + " [";
        for (x in replicas)
            str += x + "  ";
        str += " ]";
        return str;
    }
}