package x10.util.resilient.map.partition;

import x10.util.HashSet;

public class PartitionReplicationInfo {
	public val partitionId:Long;
	public val replicas:HashSet[Long];
    public def this (partId:Long, replicas:HashSet[Long]) {
    	this.partitionId = partId;
    	this.replicas = replicas;
    }
    
    public def toString():String {
    	var str:String = "";
        str += "ParitionId: " + partitionId + " [";
        for (x in replicas)
        	str += x + "  ";
        str += " ]";
    	return str;
    }
}