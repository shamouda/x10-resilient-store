import x10.util.concurrent.SimpleLatch;
import x10.util.HashMap;

//contains a HashMap object for each application map
public class Partition {
	private val moduleName = "Partition";
	public static val VERBOSE = Utils.getEnvLong("PARTITION_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
	
	public val id:Long;
	
	//application_map_Id:HashMap container
	private val maps:HashMap[String, HashMap[Any,Any]];

    private val mapsLock:SimpleLatch;

    public def this(id:Long) {
    	this.id = id;
    	maps = new HashMap[String, HashMap[Any,Any]]();
    	mapsLock = new SimpleLatch();
    }
    
    public def addMap(mapName:String) {
    	mapsLock.lock();
    	try{
    	    var appMap:HashMap[Any,Any] = maps.getOrElse(mapName,null);
    	    if (appMap == null){
    		    appMap = new HashMap[Any,Any]();
    	    }
    	    maps.put(mapName,appMap);
    	}
    	finally{
    		mapsLock.unlock();
    	}
    }
    
    public def put(mapName:String, key:Any, value:Any):Any {
    	Utils.console(moduleName, "Partition ["+id+"]  PUT ("+key+","+value+") ...");
    	val result = maps.getOrThrow(mapName).put(key,value);
    	return result;
    }

    public def get(mapName:String, key:Any) : Any {
    	Utils.console(moduleName, "Partition ["+id+"]  GET ("+key+") ...");
    	val result = maps.getOrThrow(mapName).getOrElse(key,null);
    	return result;
    }
    
}