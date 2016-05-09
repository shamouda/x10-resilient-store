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
    private val lastUsedPlaceIndex = new HashMap[Long,Long](); // node-id, last-used-place
    
    

    
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
    
    public def generateMigrationRequests(deadReplicas:ArrayList[Long]):ArrayList[MigrationRequest] {
    	val result = new ArrayList[MigrationRequest]();
    	try{
            lock.lock();
            
            val partitionsLost = new HashMap[Long,Long]();
            for (curDeadReplica in deadReplicas) {
            	val victimPartitions = placePartitions.getOrThrow(curDeadReplica);           	
            	for (partitionId in victimPartitions) {
            		val migRequest = getPartitionMigrationRequest(partitionId);
            		if (VERBOSE) Utils.console(moduleName, "MIGRATION---" + migRequest.toString());
            		result.add(migRequest);
            	} 
            }            
    	}finally {
            lock.unlock();
        }
    	return result;
    }
    
    
    private def getPartitionMigrationRequest(partitionId:Long):MigrationRequest {
    	var lastReplica:Long = -1;
    	val oldReplicas = new Rail[Long](replicationFactor);
    	val oldReplicasDead = new Rail[Boolean](replicationFactor, true);
    	val newReplicas = new Rail[Long](replicationFactor);
    	
    	var index:Long = 0;
    	var newReplicaIndex:Long = 0;
    	for (replica in replicas){
    		oldReplicas(index) = replica(partitionId);
    		if (oldReplicas(index) != -1) {
    			lastReplica = oldReplicas(index);
    			if (!Place(oldReplicas(index)).isDead()) {
    				newReplicas(newReplicaIndex++) = oldReplicas(index);
        			oldReplicasDead(index) = false;
    			}
    		}
    		index++;    		
        }
    	
        val lastUsedNodeIndex = topology.getNodeIndex(lastReplica);
        
        //temp to calculate migration places
        val tempNodePartitions= new HashMap[Long,HashSet[Long]](); // nodeId::partitions
        val tempLastUsedPlaceIndex = new HashMap[Long,Long](); // node-id, last-used-place
        
        val nodes = topology.getMainNodes();
        val nodesCount = nodes.size();
        
        for (var r:Long = newReplicaIndex; r < replicationFactor; r++){
            val nodeIndex = ( partitionId + r + lastUsedNodeIndex) % nodesCount;
            val nodeId = nodes.get(nodeIndex).getId();
            
            var nodePartitions:HashSet[Long] = tempNodePartitions.getOrElse(nodeId,null); 
            if (nodePartitions == null){
            	nodePartitions = new HashSet[Long]();
            	nodePartitions.addAll(getNodePartitions(nodeId));
            	tempNodePartitions.put(nodeId, nodePartitions);
            }
            
            if (tempLastUsedPlaceIndex.getOrElse(nodeId,-1000) == -1000){
            	tempLastUsedPlaceIndex.put(nodeId, lastUsedPlaceIndex.getOrElse(nodeId,-1));
            }
            
            if (!nodePartitions.contains(partitionId)){
                val nodePlacesCount = nodes.get(nodeIndex).places.size();
                val lastPlace = tempLastUsedPlaceIndex.getOrElse(nodeId,-1);
                val placeIndex = ( lastPlace + 1)%nodePlacesCount;
                val targetPlace = nodes.get(nodeIndex).places.get(placeIndex);
                newReplicas(newReplicaIndex++) = targetPlace.id;
                tempLastUsedPlaceIndex.put(nodeId,placeIndex);
                
                nodePartitions.add(partitionId);
            }            
        }
            
        if (newReplicaIndex < 2)
        	throw new ReplicationFailureException();
            
        for (var r:Long = newReplicaIndex; r < replicationFactor; r++){
        	newReplicas(r) = -1;
        }
        
    	return new MigrationRequest(partitionId, oldReplicas, newReplicas);
    }
}


