package x10.util.resilient.map.partition;

public class MigrationRequest (partitionId:Long, oldReplicas:Rail[Long], newReplicas:Rail[Long]) {
	
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