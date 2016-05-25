package x10.util.resilient.map;

import x10.util.*;
import x10.util.concurrent.SimpleLatch;
import x10.compiler.Inline;
import x10.xrx.Runtime;
import x10.util.concurrent.AtomicLong;
import x10.util.concurrent.AtomicBoolean;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.impl.ResilientMapImpl;
import x10.util.resilient.map.partition.PartitionTable;
import x10.util.resilient.map.common.Topology;
import x10.util.resilient.map.impl.Replica;
import x10.util.resilient.map.impl.ReplicaClient;
import x10.util.resilient.map.exception.TopologyCreationFailedException;
import x10.util.resilient.map.exception.InvalidDataStoreException;
import x10.util.resilient.map.migration.MigrationHandler;

//creates the local datastore instance  (one per place)
public class DataStore {
    private val moduleName = "DataStore";
    public static val VERBOSE = Utils.getEnvLong("DATA_STORE_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    public static val REPLICATION_FACTOR = Utils.getEnvLong("REPLICATION_FACTOR", 2);
    public static val FORCE_ONE_PLACE_PER_NODE = Utils.getEnvLong("FORCE_ONE_PLACE_PER_NODE", 0) == 1;
    public static val LEADER_NODE = Utils.getEnvLong("DATA_STORE_LEADER_NODE", 0);
    
    
    /*the data store will be invalid if:
     * - failure happended during initialization
     * - some partitions are permanantly lost due to loss of both their primary and secondary partitions
     * - leader and deputy leader are dead*/
    private var valid:AtomicBoolean = new AtomicBoolean(true);

    //takes over migration tasks
    private var leaderPlace:Place;

    //takes over migration tasks when the leader dies
    private var deputyLeaderPlace:Place;
    
    private var partitionTable:PartitionTable;
    
    private var executor:ReplicaClient;
    
    //container for the data partitions, null for non-members
    private var replica:Replica;
    
    //pointers to the different application maps
    private var userMaps:HashMap[String,ResilientMap];

    private var topology:Topology = null;
    
    private static val instance = new DataStore();
    
    private static val lock:SimpleLatch = new SimpleLatch();
    
    private static val cachedTopologyPlaceZero:Topology = createTopologyPlaceZeroOnly();
        
    private var initialized:Boolean = false;
    
    private val initLock:SimpleLatch = new SimpleLatch();
    
    private var migrationHandler:MigrationHandler;
    
    private def this() {
        userMaps = new HashMap[String,ResilientMap]();
    }
    
    public static def getInstance() : DataStore {
        if (!instance.initialized)
            instance.init();
        return instance;
    }
    
    public def init() {
        try{
            initLock.lock();
            if (!initialized) { // initialize once per place
                try{
                    if (here.id == 0) // create the topology at place 0, and copy it to other places
                        topology = cachedTopologyPlaceZero;
                    else
                        topology = at (Place(0)) { cachedTopologyPlaceZero } ;
                    if (topology == null)
                        throw new TopologyCreationFailedException();
                
                    val partitionsCount = topology.getPlacesCount();
                    val leaderNodeIndex = LEADER_NODE;
                    val placeIndex = 0;
                    leaderPlace       = topology.getPlaceByIndex(leaderNodeIndex     , placeIndex);
                    deputyLeaderPlace = topology.getPlaceByIndex(((leaderNodeIndex+1) % topology.getNodesCount()) , placeIndex);
                    partitionTable = new PartitionTable(partitionsCount, REPLICATION_FACTOR);
                    partitionTable.createPartitionTable(topology);
                    if (VERBOSE && here.id == 0)
                        partitionTable.printPartitionTable();
                    replica = new Replica(partitionTable.getPlacePartitions(here.id));
                    executor = new ReplicaClient(partitionTable);
                    
                    if (here.id == leaderPlace.id || here.id == deputyLeaderPlace.id)
                        migrationHandler = new MigrationHandler(topology, partitionTable);
                    
                       initialized = true;
        
                       if (VERBOSE) Utils.console(moduleName, "Initialization done successfully ...");
                }catch(ex:Exception) {
                    initialized = true;
                    valid.set(false);
                    Utils.console(moduleName, "Initialization failed ...");
                    ex.printStackTrace();
                }
            }
        }finally {
            initLock.unlock();
        }
    }
    
    
    public def getReplica() = replica;
    public def executor() = executor;
    
    /**
     * Returns the partition table. 
     * Called only by a MigrationHandler
     **/
    public def getPartitionTable() = partitionTable;
    
    public def getMigrationHandler() = migrationHandler;
    
    public def isLeader():Boolean {
    	var result:Boolean = false;
        lock.lock();
    	result = here.id == leaderPlace.id;
    	lock.unlock();
    	return result;
    }
    
    public def isValid() = valid.get();
    
    //TODO: handle the possibility of having some dead places
    private static def createTopologyPlaceZeroOnly():Topology {
        if (here.id == 0) {
            val topology = new Topology();
            val gr = GlobalRef[Topology](topology);
            finish for (p in Place.places()) at (p) {
                val placeId = here.id;
                var name:String = "";
                if (FORCE_ONE_PLACE_PER_NODE)
                    name = Runtime.getName(); //use process name as node name
                else
                    name = Runtime.getName().split("@")(1);
                val nodeName = name;
                at (gr.home) async {
                    atomic gr().addPlace(nodeName, placeId);
                }
            }
            return topology;
        }
        return null;
    }
    
    //should be called by one place
    public def makeResilientMap(name:String, timeoutMillis:Long):ResilientMap {
        if (!DataStore.getInstance().valid.get())
            throw new InvalidDataStoreException();
        
        var mapObj:ResilientMap = userMaps.getOrElse(name,null);
        if (mapObj == null) {
            //TODO: could not use broadcastFlat because of this exception: 
            //"Cannot create shifted activity under a SPMD Finish",
            //a shifted activity is required for copying the topology
            finish for (p in Place.places()) at (p) async {
                DataStore.getInstance().addApplicationMap(name, timeoutMillis);
            }
        }
        return userMaps.getOrThrow(name);
    }
    
    private def addApplicationMap(mapName:String, timeoutMillis:Long) {
        try{
            lock.lock();
            var resilientMap:ResilientMap = userMaps.getOrElse(mapName,null);
            if (resilientMap == null){
                replica.addMap(mapName);
                resilientMap = new ResilientMapImpl(mapName, timeoutMillis);
                userMaps.put(mapName, resilientMap);
            }
        }finally {
            lock.unlock();
        }
    }
    
    public def printTopology(){
        topology.printTopology();
    }        
   
    
    public def clientNotifyDeadPlaces(places:HashSet[Long]) {
        if (VERBOSE) Utils.console(moduleName,"clientNotifyDeadPlaces: " + Utils.hashSetToString(places));
        var targetPlace:Place;
        if (!leaderPlace.isDead()) {
            targetPlace = leaderPlace;
            if (VERBOSE) Utils.console(moduleName, "reporting dead places to LEADER: " + targetPlace);
        }
        else if (!deputyLeaderPlace.isDead()) {
            targetPlace = deputyLeaderPlace;
            if (VERBOSE) Utils.console(moduleName, "reporting dead places to DEPUTY LEADER: " + targetPlace);
        }
        else {
        	if (VERBOSE) Utils.console(moduleName, "Both LEADER and DEPUTY LEADER are dead, start searching for the leader");
        	val newLeaderId = searchForLeader();
        	if (newLeaderId == -1) {
        		if (VERBOSE) Utils.console(moduleName, "FATAL: No leader found to re-partition the data store and fix it");
        		valid.set(false);
        		throw new InvalidDataStoreException();
        	}
        	else
        		targetPlace = Place(newLeaderId);
        }
        
        val clientPlaceId = here.id;
        at (targetPlace) async {
            DataStore.getInstance().getMigrationHandler().addRequest(clientPlaceId, places);
        }
    }
    
    
    public def updateLeader(topology:Topology, partitionTable:PartitionTable) {
    	var changeDeputyLeader:Boolean = false;
    	try{
    		if (VERBOSE) Utils.console(moduleName, "Updating Leader Status (waiting for lock) ...");
    		lock.lock();   		
    		
    		this.topology.update(topology);    		
    		if (VERBOSE) Utils.console(moduleName, "Updating Leader Status - topology updated ...");
    		
    		this.partitionTable.update(partitionTable);    		
    		if (VERBOSE) Utils.console(moduleName, "Updating Leader Status - partition table updated ...");
        
    		
    		if (leaderPlace.id == here.id && deputyLeaderPlace.isDead()){
    			changeDeputyLeader = true;
    		}        
    		else if (deputyLeaderPlace.id == here.id && leaderPlace.isDead()) {
    			changeDeputyLeader = true;
    			leaderPlace = here;
    			if (VERBOSE) Utils.console(moduleName, "Promoted myself to leader ...");
    		}
    		
    		if (changeDeputyLeader) {
    			deputyLeaderPlace = findNewDeputyLeader();
    			if (VERBOSE) Utils.console(moduleName, "Changing the deputy leader to ["+deputyLeaderPlace+"] ...");
    			val tmpLeader = leaderPlace;
    			val tmpTopology = topology;        	
    			at (deputyLeaderPlace) {
    				DataStore.getInstance().updateClient (tmpLeader, here, tmpTopology);
    			}
    		}
    	}finally {
    		lock.unlock();
    	}
    }
    
    /**
     * This function should be called only form the leader or deputy leader places
     * It updates the state of other places with the same state at the leader or deputy leader.
     * Only one thread (migration handler thread) will be accessing this method at any time.
     **/
    public def updatePlaces(places:HashSet[Long]) {
        if (VERBOSE) Utils.console(moduleName, "updatePlaces: impacted client places ["+Utils.hashSetToString(places)+"] ...");
        val tmpLeader = leaderPlace;
        val tmpDeputyLeader = deputyLeaderPlace;
        val tmpTopology = topology;
        finish for (targetClient in places) {
            //leader is updated separately by updateLeader(....) 
            if (targetClient == tmpLeader.id) {
            	Utils.console(moduleName, "updatePlaces: Ignore updating client place ["+Place(targetClient)+"] because it is the leader ...");
                continue;
            }
            
            if (!Place(targetClient).isDead()) {
                at (Place(targetClient)) async {
                    DataStore.getInstance().updateClient(tmpLeader, tmpDeputyLeader, tmpTopology);
                }
            }
            else if (VERBOSE) {
                Utils.console(moduleName, "updatePlaces: Ignore updating client place ["+Place(targetClient)+"] because it is dead ...");
            }
        }
    }

    /**
     * Update my place with the updated state of leader, deputyLeader and topology
     * No need to pass in the partition table, it can be re-created using the topology object
     **/
    public def updateClient(leader:Place, deputyLeader:Place, topology:Topology) {
    	try{
    		if (VERBOSE) Utils.console(moduleName, "Updating my status with leader's new status, waiting for lock ...");
    		lock.lock();
    		val oldDeputyLeader = this.deputyLeaderPlace;
    		val oldLeader = this.leaderPlace;
    		this.leaderPlace = leader;
    		this.deputyLeaderPlace = deputyLeader;
    		this.topology.update(topology);
    		this.partitionTable.createPartitionTable(this.topology);
        
    		if (here.id == deputyLeader.id){
    			if (migrationHandler == null) {
    				migrationHandler = new MigrationHandler(topology, partitionTable);
    			}
    			else
    				migrationHandler.updateDeputyLeaderMigrationHandler(topology, partitionTable);
    		}
    		if (VERBOSE) Utils.console(moduleName, "Topology and Partition Table Updated , leader changed from["+oldLeader+"] to["+leader+"], and deputyLeader changed from ["+oldDeputyLeader+"] to ["+deputyLeader+"] ...");
    	} finally {
    		lock.unlock();
    	}
    }
    
    /**
     * Find a new place that can serve as a deputy leader
     **/
    public def findNewDeputyLeader():Place {
        var newDeputyLeader:Place = here;        
        val leaderNodeIndex = topology.getNodeIndex(here.id);        
        val nodesCount = topology.getNodesCount();
        if (VERBOSE) Utils.console(moduleName, "findNewDeputyLeader: leaderNodeIndex="+leaderNodeIndex + "  nodesCount=" + nodesCount);
        val placeIndex = 0;
        for (var i:Long = 1; i < nodesCount; i++) {
        	val nextNodeIndex = (leaderNodeIndex+i) % nodesCount ;        	
            val candidatePlace = topology.getPlaceByIndex(nextNodeIndex , placeIndex);
            if (VERBOSE) Utils.console(moduleName, "findNewDeputyLeader2: nextNodeIndex="+nextNodeIndex + "  candidatePlace=" + candidatePlace);
            if (!candidatePlace.isDead()) {
                newDeputyLeader = candidatePlace;
                break;
            }
        }
        return newDeputyLeader;
    }

    public def searchForLeader():Long {
    	if (VERBOSE) Utils.console(moduleName, "searchForLeader: waiting for lock ...");    	
    	var foundLeaderPlaceId:Long = -1;
        var deputyLeaderId:Long = -1;
    	var deputyLeaderNodeIndex:Long = -1;
    	
    	try {
    		lock.lock();    
    		if (VERBOSE) Utils.console(moduleName, "searchForLeader: obtained the lock ...");    	
    		val nodesCount = topology.getNodesCount();
    		deputyLeaderId = this.deputyLeaderPlace.id;
    		deputyLeaderNodeIndex = topology.getNodeIndex(deputyLeaderId);
    		val placeIndex = 0;
    		for (var i:Long = 1; i < nodesCount; i++) {
    			val candidatePlace = topology.getPlaceByIndex(((deputyLeaderNodeIndex+i) % nodesCount) , placeIndex);
    			if (!candidatePlace.isDead()) {
    				if (VERBOSE) Utils.console(moduleName, "Going to check if " + candidatePlace + " is a Leader ");
    				val isLeader = at (candidatePlace) DataStore.getInstance().isLeader();
    				if (VERBOSE) Utils.console(moduleName, "Is " + candidatePlace + " a Leader? " + isLeader);
    				if (isLeader) {
    					foundLeaderPlaceId = candidatePlace.id;
    					break;
    				}
    			}
    			else{
    				if (VERBOSE) Utils.console(moduleName, "searchForLeader: ["+candidatePlace+"] is dead , not leader...");
    			}
    		}
    	}finally {
    		lock.unlock();
    	}
    
    	return foundLeaderPlaceId;
    }
}
