import x10.util.*;
import x10.util.concurrent.SimpleLatch;
import x10.compiler.Inline;
import x10.xrx.Runtime;
import x10.util.concurrent.AtomicLong;


//creates the local datastore instance  (one per place)
public class DataStore {
	private val moduleName = "DataStore";
	public static val VERBOSE = Utils.getEnvLong("DATA_STORE_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	public static val REPLICATION_FACTOR = Utils.getEnvLong("REPLICATION_FACTOR", 2);
	public static val FORCE_ONE_PLACE_PER_NODE = Utils.getEnvLong("FORCE_ONE_PLACE_PER_NODE", 0) == 1;
	
	/*the data store will be invalid if some partitions are permanantly lost due to loss 
	of both their primary and secondary partitions*/
	private var valid:Boolean = true;
	
    //TODO: currently all places are members. Next, we should allow having some places are spare
	private val isMember:Boolean = true;

    //takes over migration tasks
	private var leaderPlace:Place;

    //takes over migration tasks when the leader dies
	private var deputyLeaderPlace:Place;
    
	private var partitionTable:PartitionTable;
	
	private var executor:ReplicationManager;
	
	//container for the data partitions, null for non-members
	private var container:Replica;
	
	//pointers to the different application maps
    private var userMaps:HashMap[String,ResilientMap];

    private var topology:Topology = null;
	
    private static val instance = new DataStore();
    
    private static val lock:SimpleLatch = new SimpleLatch();
    
    private static val cachedTopologyPlaceZero:Topology = createTopology();
        
    private var initialized:Boolean = false;
    
    private val initLock:SimpleLatch = new SimpleLatch();
    
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
				if (here.id == 0) // create the topology at place 0, and copy it to other places
					topology = cachedTopologyPlaceZero;
				else
					topology = at (Place(0)) { cachedTopologyPlaceZero } ;
			
				if (topology == null)
					throw new TopologyCreationFailedException();
				
		    	val partitionsCount = topology.getMainPlacesCount();

		    	
				val masterNodeIndex = 0;
				val placeIndex = 0;
				leaderPlace       = topology.getPlaceByIndex(masterNodeIndex     , placeIndex);
				deputyLeaderPlace = topology.getPlaceByIndex(masterNodeIndex + 1 , placeIndex);
		
				partitionTable = new PartitionTable(topology, partitionsCount, REPLICATION_FACTOR);
				partitionTable.createParitionTable();
				
				container = new Replica(partitionTable.getPlacePartitions(here.id));
				
				executor = new ReplicationManager(partitionTable);
		
				initialized = true;
		
				Utils.console(moduleName, "Initialization done successfully ...");
			}
		}finally {
			initLock.unlock();
		}
	}
	
	
	public def getReplica() = container;
	
	public def executor() = executor;
	
	private static def createTopology():Topology {
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
				at (gr.home) {
					atomic gr().addMainPlace(nodeName, placeId);
				}
			}
			return topology;
		}
		return null;
	}
	
	//should be called by one place
	public def makeResilientMap(name:String, timeoutMillis:Long):ResilientMap {
		var mapObj:ResilientMap = userMaps.getOrElse(name,null);
		if (mapObj == null) {
			//TODO: could not use broadcastFlat because of this exception: "Cannot create shifted activity under a SPMD Finish"
			//shifted activity is required for copying the topology
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
    
/*	
	public def getPrimaryPlace(partitionId:Long):Long {
	    val record = partitionTable.get(partitionId);
	    if (record == null)
		    return -1;
	    else 
		    return record.getPrimaryPlace();
    }
   
    public def getPartitionMapping(partitionId:Long):PartitionRecord {
 	    return partitionTable.get(partitionId);
    }
    
    
	public def toString():String {
		var str:String = "["+here+"] isMember? " + isMember + "  \n";
	    str += partitionTable.toString() + "\n";
	    if (container != null)
	    	str += container.toString() + "\n"; 
	    str += "UserMaps [";
	    val iter = userMaps.keySet().iterator();
	    while (iter.hasNext()){
	    	val key = iter.next();	
	    	str += key + ",";
	    }
	    str += "]";
	    return str;
	}
  */
	
	public def printTopology(){
		topology.printTopology();
	}		
}