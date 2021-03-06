package x10.util.resilient.map.partition;

import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicInteger;
import x10.util.HashMap;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;

//contains a HashMap object for each application map
public class Partition {
    private val moduleName = "Partition";
    
    public val id:Long;
    
    //application_map_Id::HashMap container
    private val maps:HashMap[String, HashMap[Any,VersionValue]];

    private val addMapLock:SimpleLatch;

    public def this(id:Long) {
        this.id = id;
        maps = new HashMap[String, HashMap[Any,VersionValue]]();
        addMapLock = new SimpleLatch();
    }
    
    public def this(id:Long, maps:HashMap[String, HashMap[Any,VersionValue]]) {
        this.id = id;
        this.maps = maps;
        addMapLock = new SimpleLatch();;
    }
    
    public def getMaps() = maps;
    
    public def addMap(mapName:String) {
        addMapLock.lock();
        try{
            var appMap:HashMap[Any,VersionValue] = maps.getOrElse(mapName,null);
            if (appMap == null)
                appMap = new HashMap[Any,VersionValue]();
            maps.put(mapName,appMap);
        }
        finally{
            addMapLock.unlock();
        }
    }
    
    public def put(mapName:String, key:Any, value:Any):Any {
    	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Partition ["+id+"]  PUT ("+key+","+value+") ..."); }
        var verValue:VersionValue = maps.getOrThrow(mapName).getOrElse(key,null);
        if (verValue == null)
            verValue = new VersionValue();
        verValue.updateValue(value);
        return maps.getOrThrow(mapName).put(key,verValue);
    }

    public def get(mapName:String, key:Any):Any {
    	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Partition ["+id+"]  GET ("+key+") ..."); }
        val verValue = maps.getOrThrow(mapName).getOrElse(key,null);
        return (verValue == null)? null:verValue.getValue();
    }    
    
    public def delete(mapName:String, key:Any):Any {
    	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Partition ["+id+"]  DELETE ("+key+") ..."); }
        return maps.getOrThrow(mapName).put(key, new VersionValue());//put value=null version=-1
    }

    public def getV(mapName:String, key:Any):VersionValue {
    	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Partition ["+id+"]  GET_V ("+key+") ..."); }
        return maps.getOrThrow(mapName).getOrElse(key,null);
    }   
}