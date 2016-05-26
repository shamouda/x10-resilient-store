package x10.util.resilient.map.migration;

import x10.util.concurrent.SimpleLatch;
import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.resilient.map.common.Topology;
import x10.util.resilient.map.partition.PartitionTable;
import x10.util.resilient.map.DataStore;
import x10.util.resilient.map.common.Utils;
/*
 * Responsible for receiving dead place notifications and updating the partition table
 * An object of this class exists only at the Leader and DeputyLeader places
 **/
public class MigrationHandler {
    private val moduleName = "MigrationHandler";
    public static val VERBOSE = Utils.getEnvLong("MIG_MNGR_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    public static val MIGRATION_TIMEOUT_LIMIT = Utils.getEnvLong("MIGRATION_TIMEOUT_LIMIT", 100);
    
    private val pendingRequests = new ArrayList[DeadPlaceNotification]();
    private var migrating:Boolean = false;
    private val lock = new SimpleLatch();
    
    //clones of the original objects in the DataStore class
    private val partitionTable:PartitionTable;
    private val topology:Topology;
    
    private var tmpPartitionTable:PartitionTable;    
    public def this(topology:Topology, partitionTable:PartitionTable) {
        this.partitionTable = partitionTable.clone();
        this.topology = topology.clone();
        
        this.tmpPartitionTable = partitionTable.clone(); //emulates the DataStore's partition table 
    }
    
    public def updateDeputyLeaderMigrationHandler(topology:Topology, partitionTable:PartitionTable) {
    	try{
            lock.lock();
            this.topology.update(topology);
            this.partitionTable.update(partitionTable);
            this.tmpPartitionTable.update(partitionTable);
    	}
    	finally{
    		lock.unlock();
    	}
    }
    
    public def addRequest(clientPlace:Long, deadPlaces:HashSet[Long]) {
        if (VERBOSE) Utils.console(moduleName, "adding dead place notification from client ["+clientPlace+"]");
        try{
            lock.lock();
            val impactedClients = new HashSet[Long]();
            impactedClients.add(clientPlace);
            pendingRequests.add(new DeadPlaceNotification(impactedClients, deadPlaces));
            if (!migrating) {
                if (VERBOSE) Utils.console(moduleName, "starting an async migration request ...");
                migrating = true;
                async processRequests();
            }
        }finally {
            lock.unlock();
        }
    }
    
    public def processRequests() {
        if (VERBOSE) Utils.console(moduleName, "processing migration requests ...");
        var nextReq:DeadPlaceNotification = nextRequest();
        var updateLeader:Boolean = false;
        val impactedClients = new HashSet[Long](); 
        var prevReq:Long = -1;
        var curReq:Long = 0;
        while(nextReq != null){
            if (VERBOSE) Utils.console(moduleName, "new iteration for processing migration requests ...");
            
            var newDeadPlaces:Boolean = false;
            //update the topology to re-generate a new partition table
            for (p in nextReq.deadPlaces){
                if (!topology.isDeadPlace(p)) {
                    topology.addDeadPlace(p);
                    newDeadPlaces = true;
                    updateLeader = true;
                }
            }
            
            var success:Boolean = true;
            if (newDeadPlaces || prevReq == curReq) {
                success = migratePartitions();
            }
            
            prevReq = curReq;
            if (success) {
            	tmpPartitionTable.update(partitionTable);  
                impactedClients.addAll(nextReq.impactedClients);
                /*get next request if the current one is successful*/
                nextReq = nextRequest();
                curReq++;
            }
        }       
        if (updateLeader)
        	DataStore.getInstance().updateLeader(topology, partitionTable);        
        DataStore.getInstance().updatePlaces(impactedClients);
    }
    
    //Don't aquire the lock here to allow new requests to be added while migrating the partitions
    private def migratePartitions():Boolean {
        var success:Boolean = true;
        partitionTable.createPartitionTable(topology);
        val oldPartitionTable = tmpPartitionTable;
        //1. Compare the old and new parition tables and generate migration requests
        val migrationRequests = oldPartitionTable.generateMigrationRequests(partitionTable);
        
        var newDeadPlaces:Boolean = false;
        //2. Apply the migration requests (copy from sources to destinations)
        for (req in migrationRequests) {
            if (VERBOSE) Utils.console(moduleName, "Handling migration request: " + req.toString());
            try {
                val src = req.oldReplicas.iterator().next();
                val destinations = req.newReplicas;
                val partitionId = req.partitionId;
                val gr = GlobalRef[MigrationRequest](req);
                if (VERBOSE) Utils.console(moduleName, "Copying partition from["+src+"] to["+Utils.hashSetToString(req.newReplicas)+"] ... ");
                
                if (Place(src).isDead()) {
                	newDeadPlaces = true;
                	if (VERBOSE) Utils.console(moduleName, "Copying partition from["+src+"] to["+Utils.hashSetToString(req.newReplicas)+"]  SOURCE IS DEAD ... ");
                	topology.addDeadPlace(src);
                	for (tmpReq in migrationRequests) {
                		tmpReq.start(); 
                		tmpReq.complete(false);
                	}
                	break;
                }
                else {
                	req.start(); 
                	at (Place(src)) async {
                		DataStore.getInstance().getReplica().copyPartitionsTo(partitionId, destinations, gr);                        
                	}
                }
            }
            catch(ex:Exception) {
                ex.printStackTrace();
                req.complete(false);   
            }
        }
        if (newDeadPlaces) {
        	success = false;
        }
        else {
        	success = waitForMigrationCompletion(migrationRequests, MIGRATION_TIMEOUT_LIMIT);
        	if (!success) {
        	    for (req in migrationRequests) {
                    for (dst in req.newReplicas) {
                        if (Place(dst).isDead()) {
                            if (VERBOSE) Utils.console(moduleName, "Found dead destination " + Place(dst));
                            topology.addDeadPlace(dst);
                        }
                    }
                }
        	}
        }
        if (VERBOSE) Utils.console(moduleName, "Migration completed with successStatus = ["+success+"] ...");        	
        return success;
    }
    
    private def waitForMigrationCompletion(requests:ArrayList[MigrationRequest], timeoutLimit:Long):Boolean {
        var allComplete:Boolean = true;
        do {
            allComplete = true;
            for (req in requests) {
                val timeOutFlag = req.isTimeOut(timeoutLimit);
                if (!req.isComplete() && !timeOutFlag) {
                    if (VERBOSE)  Utils.console(moduleName, "migration request {"+req.toString()+"} is not complete!!! isComplete["+req.isComplete() +"] isTimeOut["+timeOutFlag+"] ...");
                    allComplete = false;
                    break;
                }
            }
            
            if (allComplete)
                break;
            
            if (VERBOSE) Utils.console(moduleName, "waiting for migration to complete ...");
            System.threadSleep(10);
            
        } while (!allComplete);
        
        var success:Boolean = true;
        for (req in requests) {
            if (!req.isSuccessful() || (!req.isComplete() && req.isTimeOut(timeoutLimit) ) ) {
                success = false;
                break;
            }
        }
        return success;
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
                val allClients = new HashSet[Long]();
                
                for (curReq in pendingRequests) {
                    allDeadPlaces.addAll(curReq.deadPlaces);
                    allClients.addAll(curReq.impactedClients);
                }
                
                pendingRequests.clear();
                result = new DeadPlaceNotification(allClients, allDeadPlaces);
                if (VERBOSE) Utils.console(moduleName, "nextMigrationRequest is: " + result.toString());
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

class DeadPlaceNotification (impactedClients:HashSet[Long], deadPlaces:HashSet[Long]) {
	public def toString():String {
		var result:String = "ImpactedClients=[";
	    for (c in impactedClients)
	    	result += c + ",";
	    result += "]  deadPlaces[";
	    for (d in deadPlaces)
	    	result += d + ",";
	    result += "]";
		return result;
	}
	
}

