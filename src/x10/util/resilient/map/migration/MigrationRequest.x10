package x10.util.resilient.map.migration;

import x10.util.HashSet;

public class MigrationRequest (partitionId:Long, oldReplicas:HashSet[Long], newReplicas:HashSet[Long]) {
	
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