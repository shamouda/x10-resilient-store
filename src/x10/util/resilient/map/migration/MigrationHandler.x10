package x10.util.resilient.map.migration;

import x10.util.concurrent.SimpleLatch;

/*
 * Responsible for receiving dead place notifications and updating the partition table
 * An object of this class exists only at the Leader and DeputyLeader places
 **/
public class MigrationHandler {
    private val pendingRequests = new ArrayList[DeadPlaceNotificationRequest]();
    private var migrating:Boolean = false;
    private val lock = new SimpleLatch();
    
    //clones of the original objects in the DataStore class
    private val partitionTable:PartitionTable;
    private val topology:Topology;
    
    
    public def this(topology:Topology, partitionTable:PartitionTable) {
    	this.partitionTable = partitionTable.clone();
    	this.topology = topology.clone();
    }
    
    public def addRequest(clientPlace:Long, deadPlaces:HashSet[Long]) {
    	try{
    		lock.lock();
    		pendingRequest.add(new DeadPlaceNotification(clientPlace, deadPlaces));
    		if (!migrating) {
    			migrating = true;
    			async processRequests();
    		}
    	}finally {
    		lock.unlock();
    	}
    }
    
    public def processRequests() {
    	var nextReq:DeadPlaceNotification = nextRequest();
    	var newDeadPlaces:Boolean = false;
    	while(nextReq != null){
    		for (p in nextReq.deadPlaces){
    			if (!topology.isDeadPlace(p)) {
    				topology.addDeadPlace(p);
    				newDeadPlaces = true;
    			}
    		}
    		
    		
    		migratePartitions();
    		
    		nextReq = nextRequest();
    	}
    }
    
    //Don't aquire the lock here to allow new requests to be added while migrating the partitions
    private def migratePartitions() {
    	partitionTable.createPartitionTable(topology);
    	val oldPartitionTable = DataStore.getInstance().getPartitionTable();
    	//1. Compare the old and new parition tables and generate migration requests
    	val migrationRequests = oldPartitionTable.generateMigrationRequests(partitionTable);
    	
    	//2. Apply the migration requests (copy from sources to destinations)
    	for (req in migrationRequests) {
    		try {
    			val src = req.oldReplicas.get(0);
    			for (dst in req.newReplicas) {
    				at (Place(src)) {
    					//TODO: how to make sure the parition is not being updated while copying it????????????????
    					val partition = DataStore.getInstance().getReplica().getPartition(req.partitionId);
    					at (Place(dst)) {
    						DataStore.getInstance().getReplica().addPartition(partition);
    					}
    				}
    			}
    		}
    		catch(ex:Exception) {
    			
    		}
    	}    	
    }
    
    
    
    public def isMigrating() {
    	var result:Boolean = false;
    	try{
    		lock.lock();
    		result = migrating;
    	}
    	finally {
    		lock.unlock();
    	}
    	return result;
    }
    
    private def nextRequest():DeadPlaceNotification{
    	var result:DeadPlaceNotification = null;
    	try{
    		lock.lock();
    		if (pendingRequests.size() > 0) {
    			//combine all requests into one
    			val allDeadPlaces = new HashSet[Long]();
    			for (curReq in pendingRequests)
    				allDeadPlaces.addAll(curReq.deadPlaces);
    			
    			pendingRequests.clear();
    			result = new DeadPlaceNotification(-1, allDeadPlaces);
    		}
    		else
    			migrating = false;
    	}
    	finally{
    		lock.unlock();
    	}
    	return result;
    }
    
}

class DeadPlaceNotification (clientPlace:Long, deadPlaces:HashSet[Long]) {}

