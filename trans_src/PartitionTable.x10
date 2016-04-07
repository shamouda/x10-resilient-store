import x10.util.HashMap;
import x10.util.concurrent.SimpleLatch;
import x10.util.ArrayList;
import x10.util.HashSet;

public class PartitionTable {
	private var partitionsCount:Long; //a partition for each main place
    private val replicationFactor:Long;
    private val topology:Topology;
    // partition index :  0  -   1  -   2    -   3   -   4
    // replica 1 place :  1  -   1  -   2    -   2   -    3
    // replica 2 place :  2  -   3  -   1    -   1   -    2
	private val replicas:ArrayList[Rail[Long]] = new ArrayList[Rail[Long]]();

	private val lock:SimpleLatch;

    private val nodePartitions= new HashMap[Long,HashSet[Long]](); // nodeId::partitions
	
    public def this(topology:Topology, replicationFactor:Long) {    	
    	this.lock = new SimpleLatch();
    	this.replicationFactor = replicationFactor;
    	this.topology = topology;
    }
    
    public def createParitionTable() {
    	partitionsCount = topology.getMainPlacesCount();
    	
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
					nodePartitions.add(p);					
					Console.OUT.println("partition:"+p + "   r:" + r + "   nIndex:"+nodeIndex + "   nodeId:"+nodeId + "   lastPlace:" + lastPlace + "  placeIndex:"+placeIndex);
				}
				else{
					Console.OUT.println("partition:"+p + "   r:" + r + "   nIndex:"+nodeIndex + "   nodeId:"+nodeId);	
				}
    		}
    		
    		if (replicaIndex < 2)
    			throw new ReplicationFailureException();
    		
    		for (var r:Long = replicaIndex; r < replicationFactor; r++){
    			replicas.get(r)(p) = -1;
    		}
    	}
    	
    	
    	Console.OUT.println("Node partition mapping: ");
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
    	
    }
    
    public def getNodePartitions(nodeId:Long):HashSet[Long] {
    	var obj:HashSet[Long] = nodePartitions.getOrElse(nodeId,null);
    	if (obj == null){
    		obj = new HashSet[Long]();
    		nodePartitions.put(nodeId, obj);
    	}
    	return obj;
    }
    
    public def printParitionTable() {
    	for (var p:Long = 0; p < partitionsCount; p++) {
    		var str:String = "";
    	    for (var r:Long = 0; r < replicationFactor; r++) {
    	    	str += replicas.get(r)(p) + " - ";
    	    }
    	    Console.OUT.println(p + " => " + str);
    	}
    }
}
