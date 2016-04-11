import x10.util.HashMap;
import x10.util.ArrayList;

/*
 * Used to log the changes in key values by a single transaction
 * The object is expected to be used by only one thread
 * */
//transaction status: started - ready to commit - commited - aborted
public class TransLog {
	private val moduleName = "TransLog";
	public static val VERBOSE = Utils.getEnvLong("TRANS_LOG_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
	public val transId:Long;
	//the used keys
    private val cache:HashMap[Any,TransCachedRecord] = new HashMap[Any,TransCachedRecord]();

	private val startTimeMillis:Long;
	
	public val clientPlaceId:Long;
	
	public def this(transId:Long, startTimeMillis:Long, clientPlaceId:Long){
		this.transId = transId;
		this.startTimeMillis = startTimeMillis;
		this.clientPlaceId = clientPlaceId;
	}
	
	//must be called before update to store the initial value
	public def logGet (key:Any, initVersion:Int, initValue:Any) {
		var cacheRec:TransCachedRecord = cache.getOrElse(key,null);
		if (cacheRec == null) {
			cacheRec = new TransCachedRecord(initVersion, initValue);
			cache.put(key, cacheRec);
		}
		cache.put(key,cacheRec);
	}
	
	public def logUpdate(key:Any, initVersion:Int, initValue:Any, newValue:Any) {
		var cacheRec:TransCachedRecord = cache.getOrElse(key,null);
		if (cacheRec == null) {
			cacheRec = new TransCachedRecord(initVersion, initValue);
			cache.put(key, cacheRec);
		}
		cacheRec.update(newValue);
	}
	
	public def logDelete(key:Any, initVersion:Int, initValue:Any) {
		var cacheRec:TransCachedRecord = cache.getOrElse(key,null);
		if (cacheRec == null) {
			cacheRec = new TransCachedRecord(initVersion, initValue);
			cache.put(key, cacheRec);
		}
		cacheRec.delete();
	}
	
	
	public def isConflicting (other:TransLog):Boolean {
		var result:Boolean = false;
	    val overlap = getOverlappingKeys(other);
	    for (key in overlap){
	    	if (!cache.getOrThrow(key).readOnly() || !other.cache.getOrThrow(key).readOnly()){
	    		if (VERBOSE) Utils.console(moduleName, "Tx("+transId+") and Tx("+(other.transId)+") conflict in key ["+key+"]");
	    		result = true;
	    		break;
	    	}
	    }
	    if (VERBOSE) Utils.console(moduleName, "Tx("+transId+") and Tx("+(other.transId)+") not conflicting ...");
		return result;
	}
	
	private def getOverlappingKeys(other:TransLog):ArrayList[Any] {
		val iter = cache.keySet().iterator();
		val list = new ArrayList[Any]();
		while (iter.hasNext()){
			val key = iter.next();
			try{
				other.cache.getOrThrow(key);
				list.add(key);
			}catch(ex:Exception){}
		}
		
		if (VERBOSE){
			if (list.size() == 0)
				Utils.console(moduleName,"Tx("+transId+") and Tx("+(other.transId)+") no overlap");
			else{
				var str:String = "";
				for (x in list)
					str += x + ",";
				Utils.console(moduleName,"Tx("+transId+") and Tx("+(other.transId)+") overlapped keys = " + str);
			}
		}
		return list;
	}

}