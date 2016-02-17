import x10.util.*;
import x10.util.concurrent.SimpleLatch;
import x10.compiler.Inline;

//Equivelent to HazelcastDataStore
//creates the local datastore instance  (one per place)

//TODO: add these functions
//ds.getPlaces();
//Runtime.joinResilientDataStore()
public class DataStore {
	
	//we assume initial contigous places are used
	public static val DS_FIRST_PLACE:Long = getEnvLong("X10_DATA_STORE_FIRST_PLACE", 0); //The initial leader
	public static val DS_INITIAL_MEMBERS_COUNT:Long = getEnvLong("X10_DATA_STORE_PLACE_COUNT", Place.numPlaces()); //initial members count
	public static val DS_PARTITIONS_COUNT:Long = getEnvLong("X10_DATA_STORE_PARTITION_COUNT", 271); //The initial leader
	public static val DS_PARTITIONS_MAX_RETRY:Long = getEnvLong("X10_DATA_STORE_MAX_RETRY", 10);
	
	/*the data store will be invalid if some partitions are permanantly lost due to loss 
	of both their primary and secondary partitions*/
	private var valid:Boolean = true;
	
	private var isMember:Boolean = false;
	
	//changes when the leader dies the leader dies
	private var leaderPlace:Long = -1;
	
	private var places:PlaceGroup;
    
	private var partitionTable:PartitionTable;
	
	//container for the data partitions, null for non-members
	private var container:Container;
	
	//pointers to the different application maps
    private var userMaps:HashMap[String,ResilientMap];
	
    private static val instance:DataStore = new DataStore();
    
    private static val lock = new SimpleLatch();
    
	private def this() {
		assure(DS_FIRST_PLACE>=0 && Place.numPlaces() >= (DS_FIRST_PLACE + DS_INITIAL_MEMBERS_COUNT), 
				"Invalid Resilient Data Store Configurations");
		val segments = new Rail[Int](DS_INITIAL_MEMBERS_COUNT, (i:Long)=>compPartitionsCount(DS_PARTITIONS_COUNT, DS_INITIAL_MEMBERS_COUNT, i as Int) as Int);
		
		leaderPlace = DS_FIRST_PLACE;
		
		places = makeInitialPlaceGroup(DataStore.DS_FIRST_PLACE, DataStore.DS_PARTITIONS_COUNT);
	    
		partitionTable = new PartitionTable(segments);
		
		userMaps = new HashMap[String,ResilientMap]();
		
		if (here.id >= DS_FIRST_PLACE && here.id < (DS_FIRST_PLACE+DS_INITIAL_MEMBERS_COUNT)){
			isMember = true;
			container = new Container(segments);
		}
	}
	
	public static def getInstance() = instance;
	
	public def getContainer() = container;
	
	//should be called by one place
	public def makeResilientMap(name:String, strong:Boolean):ResilientMap {
		var mapObj:ResilientMap = userMaps.getOrElse(name,null);
		if (mapObj == null) {
			Place.places().broadcastFlat(()=>{
				try{
					DataStore.getInstance().lock.lock();
					var resilientMap:ResilientMap = DataStore.getInstance().userMaps.getOrElse(name,null);
					if (resilientMap == null){
						resilientMap = new ResilientMap(name, strong);
						DataStore.getInstance().userMaps.put(name, resilientMap);
					}
				}finally {
					DataStore.getInstance().lock.unlock();
				}
			}, (Place)=>true);
		}
		return userMaps.getOrElse(name,null);
	}
	
    //TODO: move to PlaceGroupBuilder
    static def makeInitialPlaceGroup(firstPlace:Long, count:Long):PlaceGroup {
        val pg = new x10.util.ArrayList[Place]();
        for (var i:Long = firstPlace; i < count; i++){
            pg.add(Place(i));
        }
        var placeGroup:SparsePlaceGroup = new SparsePlaceGroup(pg.toRail());
        return placeGroup;
    }
    
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
    
	
	
	/***************************    Utils    ******************************/
	
    public static def getEnvLong(name:String, defaultVal:Long) {
        val env = System.getenv(name);
        val v = (env!=null) ? Long.parseLong(env) : defaultVal;
        return v;
    }
    
    public static def assure(v:Boolean, msg:String) {
		if (!v) {
			val fmsg = "Assertion fail at P" + here.id()+" - "+msg;
			throw new UnsupportedOperationException(fmsg);
		}
	}
    
    @Inline   //copied from x10.matrix.block.Grid
    static def compPartitionsCount(pTotal:Long, membersCnt:Long, memberIdx:Long):Long {
        var sz:Long = (memberIdx < pTotal % membersCnt)?1:0;
        sz += pTotal / membersCnt;
        return sz;
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
    
		
}