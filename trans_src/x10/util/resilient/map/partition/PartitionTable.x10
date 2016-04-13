package x10.util.resilient.map.partition;

import x10.util.HashMap;
import x10.util.concurrent.SimpleLatch;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.exception.ReplicationFailureException;

public class PartitionTable {
	private val moduleName = "PartitionTable";
	public static val VERBOSE = Utils.getEnvLong("PART_TABLE_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
	private var partitionsCount:Long; //a partition for each main place
    private val replicationFactor:Long;
    private val topology:Topology;
    // partition index :  0  -   1  -   2    -   3   -   4
    // replica 1       :  1  -   2  -   3    -   4   -   5
    // replica 2       :  2  -   3  -   4    -   5   -   0
	private val replicas:ArrayList[Rail[Long]] = new ArrayList[Rail[Long]]();

	private val lock:SimpleLatch;

    //for easier lookup
    private val nodePartitions= new HashMap[Long,HashSet[Long]](); // nodeId::partitions
    private val placePartitions= new HashMap[Long,HashSet[Long]](); // placeId::partitions
    
    public def this(topology:Topology, partitionsCount:Long, replicationFactor:Long) {    	
    	this.lock = new SimpleLatch();
    	this.replicationFactor = replicationFactor;
    	this.topology = topology;
    	this.partitionsCount = partitionsCount;
    }
    
    public def createParitionTable() {
    	for (i in 0..(replicationFactor-1)){
    		replicas.add(new Rail[Long](partitionsCount));
    	}
    	
    	val nodes = topology.getMainNodes();
    	val nodesCount = nodes.size();
    	val lastUsedPlaceIndex = new HashMap[Long,Long](); // node-id, lasy-used-place
    	
    	for (var p:Long = 0; p < partitionsCount; p++) {
    		var replicaIndex:Long = 0;
    		for (var r:Long = 0; r < replicationFactor; r++){    			
				val nodeIndex = ( p + r ) % nodesCount;
				val nodeId = nodes.get(nodeIndex).getId();
				
				val nodePartitions = getNodePartitions(nodeId);
				if (!nodePartitions.contains(p)){
					val nodePlacesCount = nodes.get(nodeIndex).places.size();
					val lastPlace = lastUsedPlaceIndex.getOrElse(nodeId,-1);
					val placeIndex = ( lastPlace + 1)%nodePlacesCount;
					val targetPlace = nodes.get(nodeIndex).places.get(placeIndex);
					replicas.get(replicaIndex++)(p) = targetPlace.id;
					lastUsedPlaceIndex.put(nodeId,placeIndex);
					
					getPlacePartitions(targetPlace.id).add(p);
					nodePartitions.add(p);
					if (VERBOSE) Utils.console(moduleName, "partition:"+p + "   r:" + r + "   nIndex:"+nodeIndex + "   nodeId:"+nodeId + "   lastPlace:" + lastPlace + "  placeIndex:"+placeIndex);
				}
				else{
					if (VERBOSE) Utils.console(moduleName, "partition:"+p + "   r:" + r + "   nIndex:"+nodeIndex + "   nodeId:"+nodeId);	
				}
    		}
    		
    		if (replicaIndex < 2)
    			throw new ReplicationFailureException();
    		
    		for (var r:Long = replicaIndex; r < replicationFactor; r++){
    			replicas.get(r)(p) = -1;
    		}
    	}
    	
    	
    	if (VERBOSE) { 
    		Utils.console(moduleName, "Node partitions ");
    		val iter = nodePartitions.keySet().iterator();
    		while (iter.hasNext()) {
    			val key = iter.next();
    			val mySet = nodePartitions.get(key);
    			val iter2 = mySet.iterator();
    			var str:String = "";
    			while (iter2.hasNext())
    				str += iter2.next() + " - ";
    			Console.OUT.println(key + "->>> " + str);
    		}
    	
    	
    		Utils.console(moduleName, "Place partitions ");
    		val piter = placePartitions.keySet().iterator();
    		while (piter.hasNext()) {
    			val key = piter.next();
    			val mySet = placePartitions.get(key);
    			val iter2 = mySet.iterator();
    			var str:String = "";
    			while (iter2.hasNext())
    				str += iter2.next() + " - ";
    			Console.OUT.println(key + "->>> " + str);
    		}
    	}
    	
    	
    }
    
    public def getNodePartitions(nodeId:Long):HashSet[Long] {
    	var obj:HashSet[Long] = null;
    	try{
    		lock.lock();
    		obj = nodePartitions.getOrElse(nodeId,null);
    		if (obj == null){
    			obj = new HashSet[Long]();
    			nodePartitions.put(nodeId, obj);
    		}
    	}
    	finally {
    		lock.unlock();
    	}
    	return obj;
    }
    
    public def getPlacePartitions(placeId:Long):HashSet[Long] {
    	var obj:HashSet[Long] = null;
    	try {
    		lock.lock();
    		obj = placePartitions.getOrElse(placeId,null);
    		if (obj == null){
    			obj = new HashSet[Long]();
    			placePartitions.put(placeId, obj);
    		}
    	}finally {
    		lock.unlock();
    	}
    	return obj;
    }
    
    
    public def getKeyReplicas(key:Any):PartitionReplicationInfo {
    	var result:PartitionReplicationInfo = null;
    	try{
    		lock.lock();
    		var partitionId:Long = key.hashCode() as Long;
    		partitionId = partitionId % partitionsCount ;
    		val keyReplicas = new HashSet[Long]();
    		for (replica in replicas){
    			keyReplicas.add(replica(partitionId));
    		}
    		result = new PartitionReplicationInfo(partitionId, keyReplicas);
    	}finally {
    		lock.unlock();
    	}
    	return result;
    }
    
     
    private def getPartitionId(key:Any) : Long {
    	var result:Long = -1;
        try {
        	lock.lock();
        	result = key.hashCode() as Long % partitionsCount;
        }
        finally {
        	lock.unlock();
        }
    	return result;
    }
    
    public def printParitionTable() {
    	Utils.console(moduleName, "Parition places");
    	for (var p:Long = 0; p < partitionsCount; p++) {
    		var str:String = "";
    	    for (var r:Long = 0; r < replicationFactor; r++) {
    	    	str += replicas.get(r)(p) + " - ";
    	    }
    	    Console.OUT.println(p + " => " + str);
    	}
    }
}


