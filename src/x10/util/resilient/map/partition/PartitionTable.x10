package x10.util.resilient.map.partition;

import x10.util.HashMap;
import x10.util.concurrent.SimpleLatch;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.exception.ReplicationFailureException;
import x10.util.resilient.map.exception.InvalidDataStoreException;
import x10.util.resilient.map.impl.migration.MigrationRequest;
import x10.util.concurrent.AtomicLong;

/*
 * The partition table maps each partition to the places that contain it.
 * The replicationFactor property specifies the maximum number of available copies.
 * If the replicactionFactor is 2, we create two rails to store the two places that have the copies of each partition.
 *    partition index :  0  -   1  -   2    -   3   -   4
 *    replica 1       :  1  -   2  -   3    -   4   -   5
 *    replica 2       :  2  -   3  -   4    -   5   -   0    
 * 
 * Each place creates its partition table individually given the Topology object.
 **/
public class PartitionTable (partitionsCount:Long, replicationFactor:Long) {
    private val moduleName = "PartitionTable";
    public static val VERBOSE = Utils.getEnvLong("PART_TABLE_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;

    /*The replication arrays*/
    private val replicas:ArrayList[Rail[Long]] = new ArrayList[Rail[Long]]();

    private val lock:SimpleLatch = new SimpleLatch();

    //maps a nodeId to partitions it hosts
    private var nodePartitions:HashMap[Long,HashSet[Long]];
    
    //maps a placeId to partitions it hosts 
    private var placePartitions:HashMap[Long,HashSet[Long]];
    
    private val version:AtomicLong = new AtomicLong(0);
    
    public def getVersion() = version.get();
    
    /* Creates the partition replica mapping
     * When places die, this method should be called with an updated topology 
     * retrieved from leader/deputyLeader place
     */
    public def createPartitionTable(topology:Topology) {
        if (here.id == 0) {
            topology.printTopology();
            topology.printDeadPlaces();
        }
            
        replicas.clear();
        for (i in 0..(replicationFactor-1)){
            replicas.add(new Rail[Long](partitionsCount));
        }
        val nodes = topology.getNodes();
        val nodesCount = nodes.size();
        val lastUsedPlace = new HashMap[Long,Long](); // node-id, last-used-place
        nodePartitions = new HashMap[Long,HashSet[Long]](); 
        placePartitions = new HashMap[Long,HashSet[Long]](); // placeId::partitions
        
        for (var p:Long = 0; p < partitionsCount; p++) {
            var replicaIndex:Long = 0;
            var d:Long = 0;
            for (var r:Long = 0; r < replicationFactor; r++){
                val nodeIndex = ( p + r + d) % nodesCount; //when no places are dead, d will always be 0
                val nodeId = nodes.get(nodeIndex).id;
                val node = nodes.get(nodeIndex);
                val nodePartitions = getNodePartitions(nodeId);
                if (!nodePartitions.contains(p)){
                    val nodePlacesCount = node.places.size();
                    val lastPlace = lastUsedPlace.getOrElse(nodeId,-1);
                    var placeFound:Boolean = false;
                    //try all places within the node (don't overload the place that was last used)
                    for (var i:Long = 0; i < nodePlacesCount; i++) {
                        val placeIndex = ( lastPlace + i + 1) % nodePlacesCount;
                        val targetPlace = nodes.get(nodeIndex).places.get(placeIndex);
                        
                        //topology.isDeadPlace returns the same result at all places because they 
                        //use the same 'topology' object
                        if (!topology.isDeadPlace(targetPlace.id)) {
                            replicas.get(replicaIndex++)(p) = targetPlace.id;
                            lastUsedPlace.put(nodeId, placeIndex);
                            getPlacePartitions(targetPlace.id).add(p);
                            nodePartitions.add(p);
                            placeFound = true;
                            break;
                        }
                    }
                    if (!placeFound) {
                        //some places are dead in this node, try the next one
                        d++;
                        r--; 
                    }
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
                Console.OUT.println("Node"+key + "->>> Part-" + str);
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
                Console.OUT.println("Place"+key + "->>> Part-" + str);
            }
        }
    }
    
    public def getNodePartitions(nodeId:Long):HashSet[Long] {
        var obj:HashSet[Long] = null;
        try{
            lock.lock();
            obj = nodePartitions.getOrElse(nodeId,new HashSet[Long]());
            nodePartitions.put(nodeId, obj);
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
    
    
    public def getKeyReplicas(key:Any):PartitionReplicas {
        var result:PartitionReplicas = null;
        try{
            lock.lock();
            var partitionId:Long = key.hashCode() as Long  % partitionsCount ;
            val keyReplicas = new HashSet[Long]();
            for (replica in replicas){
                keyReplicas.add(replica(partitionId));
            }
            result = new PartitionReplicas(partitionId, keyReplicas);
        }finally {
            lock.unlock();
        }
        return result;
    }
    
    /*
     * Compares two parition tables and generates migration requests
     **/
    public def generateMigrationRequests(updatedTable:PartitionTable):ArrayList[MigrationRequest] {
        if (VERBOSE) {
            Console.OUT.println("%%%   Old Table   %%%");
            printPartitionTable();        
            Console.OUT.println("%%%   New Table   %%%");
            updatedTable.printPartitionTable();
            Console.OUT.println("%%%%%%%%%%%%%%%%%%%%%%");
        }
        val result = new ArrayList[MigrationRequest]();
        try {
            lock.lock();
            for (var p:Long = 0; p < partitionsCount; p++) {
                val hostPlaces = new HashSet[Long]();
                val newPlaces = new HashSet[Long]();
                for (var new_r:Long = 0; new_r < replicationFactor; new_r++) {
                    var found:Boolean = false;
                    for (var old_r:Long = 0; old_r < replicationFactor; old_r++) {
                        if (replicas.get(old_r)(p) == updatedTable.replicas.get(new_r)(p)) {
                            found = true;
                            hostPlaces.add(replicas.get(new_r)(p));
                            break;
                        }
                    }
                    if (!found){
                        newPlaces.add(updatedTable.replicas.get(new_r)(p));
                    }
                }
                
                if (hostPlaces.size() == 0)
                    throw new InvalidDataStoreException();
                
                if (newPlaces.size() > 0)                
                    result.add(new MigrationRequest(p, hostPlaces, newPlaces));                
            }
        } finally {
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
    
    public def printPartitionTable() {
        Utils.console(moduleName, "Parition places");
        for (var p:Long = 0; p < partitionsCount; p++) {
            var str:String = "";
            for (var r:Long = 0; r < replicationFactor; r++) {
                str += replicas.get(r)(p) + " - ";
            }
            Console.OUT.println(p + " => " + str);
        }
    }
    
    public def clone():PartitionTable {
        val cloneObj = new PartitionTable(partitionsCount, replicationFactor);
        cloneObj.replicas.addAll(replicas);
        if (nodePartitions != null) {
            cloneObj.nodePartitions = new HashMap[Long,HashSet[Long]]();
            val iter = nodePartitions.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val value = nodePartitions.getOrThrow(key);
                cloneObj.nodePartitions.put(key, value);
            }
        }
        if (placePartitions != null) {
            cloneObj.placePartitions = new HashMap[Long,HashSet[Long]]();
            val iter2 = placePartitions.keySet().iterator();
            while (iter2.hasNext()) {
                val key = iter2.next();
                val value = placePartitions.getOrThrow(key);
                cloneObj.placePartitions.put(key,value);
            }
        }
        return cloneObj;
    }
    
    public def update(newTable:PartitionTable) {
        lock.lock();
        for (var r:Long = 0; r < replicationFactor; r++) {
            for (var p:Long = 0; p < partitionsCount; p++) {
                replicas.get(r)(p) = newTable.replicas.get(r)(p) ; 
            }
        }
        nodePartitions.clear();
        
        val iter = newTable.nodePartitions.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val value = newTable.nodePartitions.getOrThrow(key);
            nodePartitions.put(key, value);
        }
        
        placePartitions.clear();
        
        val iter2 = newTable.placePartitions.keySet().iterator();
        while (iter2.hasNext()) {
            val key = iter2.next();
            val value = newTable.placePartitions.getOrThrow(key);
            placePartitions.put(key, value);
        }        
        lock.unlock();        
        version.incrementAndGet();
    }
}




