import x10.util.concurrent.SimpleLatch;
import x10.util.HashMap;

//contains a HashMap object for each application map
public class Partition {
	public val id:Long;
	
	//application_map_Id:HashMap container
	private val maps:HashMap[String, HashMap[Any,Any]];

    private val lock:SimpleLatch;

    public def this(id:Long, isPrimary:Boolean) {
    	this.id = id;
    	maps = new HashMap[String, HashMap[Any,Any]]();
    	lock = new SimpleLatch();
    	
    }
    
    //We might not need this one, because put makes the same logic
    public def addMap(mapName:String) {
    	lock.lock();
    	try{
    	    var appMap:HashMap[Any,Any] = maps.getOrElse(mapName,null);
    	    if (appMap == null){
    		    appMap = new HashMap[Any,Any]();
    	    }
    	    maps.put(mapName,appMap);
    	}
    	finally{
    		lock.unlock();
    	}
    }
    
    public def put(mapName:String, key:Any, value:Any) {
    	Console.OUT.println("Partition ["+id+"]  PUT ("+key+","+value+") ...");
    	lock.lock();
    	try{
    	    var appMap:HashMap[Any,Any] = maps.getOrElse(mapName,null);
    	    if (appMap == null){
    	    	appMap = new HashMap[Any,Any]();
    	    }
    	    appMap.put(key,value);
    	    maps.put(mapName,appMap);
    	}
    	finally{
    		lock.unlock();
    	}
    }

    public def get(mapName:String, key:Any) : Any {
    	var result:Any = null;
    	lock.lock();
    	try{
    	    val appMap = maps.getOrElse(mapName,null);
    	    if (appMap == null){
    		    result = null;
    	    }
    	    else {
    	    	result = appMap.getOrElse(key,null);
    	    }
    	}
    	finally{
    		lock.unlock();
    	}
    	return result;
    }

}