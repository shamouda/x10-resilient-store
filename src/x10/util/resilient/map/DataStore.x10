package x10.util.resilient.map;

import x10.util.*;
import x10.util.concurrent.SimpleLatch;
import x10.compiler.Inline;
import x10.xrx.Runtime;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.impl.ResilientMapImpl;
import x10.util.resilient.map.partition.PartitionTable;
import x10.util.resilient.map.partition.Topology;
import x10.util.resilient.map.impl.Replica;
import x10.util.resilient.map.impl.ReplicaClient;
import x10.util.resilient.map.exception.TopologyCreationFailedException;
import x10.util.resilient.map.exception.InvalidDataStoreException;

//creates the local datastore instance  (one per place)
public class DataStore {
    private val moduleName = "DataStore";
    public static val VERBOSE = Utils.getEnvLong("DATA_STORE_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    public static val REPLICATION_FACTOR = Utils.getEnvLong("REPLICATION_FACTOR", 2);
    public static val FORCE_ONE_PLACE_PER_NODE = Utils.getEnvLong("FORCE_ONE_PLACE_PER_NODE", 0) == 1;
    
    /*the data store will be invalid if:
     * - failure happended during initialization
     * - some partitions are permanantly lost due to loss of both their primary and secondary partitions*/
    private var valid:Boolean = true;

    //takes over migration tasks
    private var leaderPlace:Place;

    //takes over migration tasks when the leader dies
    private var deputyLeaderPlace:Place;
    
    private var partitionTable:PartitionTable;
    
    private var executor:ReplicaClient;
    
    //container for the data partitions, null for non-members
    private var container:Replica;
    
    //pointers to the different application maps
    private var userMaps:HashMap[String,ResilientMap];

    private var topology:Topology = null;
    
    private static val instance = new DataStore();
    
    private static val lock:SimpleLatch = new SimpleLatch();
    
    private static val cachedTopologyPlaceZero:Topology = createTopologyPlaceZeroOnly();
        
    private var initialized:Boolean = false;
    
    private val initLock:SimpleLatch = new SimpleLatch();
    
    private var migrating:Boolean = false;
    
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

                
            			val leaderNodeIndex = 0;
            			val placeIndex = 0;
            			leaderPlace       = topology.getPlaceByIndex(leaderNodeIndex     , placeIndex);
            			deputyLeaderPlace = topology.getPlaceByIndex(leaderNodeIndex + 1 , placeIndex);
        
            			partitionTable = new PartitionTable(partitionsCount, REPLICATION_FACTOR);
            			partitionTable.createPartitionTable(topology);
            			
            			if (VERBOSE && here.id == 0)
            				partitionTable.printParitionTable();
            			
            			container = new Replica(partitionTable.getPlacePartitions(here.id));
                
            			executor = new ReplicaClient(partitionTable);
        
            			initialized = true;
        
            			if (VERBOSE) Utils.console(moduleName, "Initialization done successfully ...");
            	}catch(ex:Exception) {
            		initialized = true;
            		valid = false;
            		Utils.console(moduleName, "Initialization failed ...");
            		ex.printStackTrace();
            	}
            }
        }finally {
            initLock.unlock();
        }
    }
    
    
    public def getReplica() = container;
    public def executor() = executor;
    
    //TODO: handle the possibility of having some dead places
    private static def createTopologyPlaceZeroOnly():Topology {
        if (here.id == 0) {
            val topology = new Topology();
            val gr = GlobalRef[Topology](topology);
            finish for (p in Place.places()) at (p) async {
                val placeId = here.id;
                var name:String = "";
                if (FORCE_ONE_PLACE_PER_NODE)
                    name = Runtime.getName();
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
    	if (!DataStore.getInstance().valid)
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
                container.addMap(mapName);
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
    		valid = false;
    		throw new InvalidDataStoreException();
    	}
    	
    	val clientPlaceId = here.id;
    	at (targetPlace) async {
    		val ds = DataStore.getInstance();
    		try {
    			lock.lock();    			
    			
    			
    			//TODO: add dead places notification
    			
    		}
    		finally{
    			lock.unlock();
    		}
    		
    	}
    	
    }

    
    private def promoteHereToLeader() {
        leaderPlace       = here;
        deputyLeaderPlace = here;
        
        //try to appoint a deputy leader
        try{
        	val leaderNodeIndex = topology.getNodeIndex(here.id);
        	val nodesCount = topology.getNodesCount();
        	val placeIndex = 0;
        	for (var i:Long = leaderNodeIndex+1; i < nodesCount; i++) {
        		val candidatePlace = topology.getPlaceByIndex(leaderNodeIndex + 1 , placeIndex);
        		if (!candidatePlace.isDead()) {
        			promoteToDeputyLeader(deputyLeaderPlace);
        			deputyLeaderPlace = candidatePlace;
        			break;
        		}
        	}
        }catch(ex:Exception) {
        	ex.printStackTrace();
        }
    }
    
    private def promoteToDeputyLeader(place:Place) {
    	at (place) {
    		DataStore.getInstance().deputyLeaderPlace = here;
    		//TODO: we can also update the partition table and topology here
    	}
    }
}
