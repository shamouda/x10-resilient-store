import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;

public class MapRequest {
	
	private val moduleName = "MapRequest";
	public static val VERBOSE = Utils.getEnvLong("MAP_REQ_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
	
	public static val REQ_COMMIT:Int = 1n;
	public static val REQ_ABORT:Int = 2n;
	public static val REQ_GET:Int = 3n;
    public static val REQ_PUT:Int = 4n;
    public static val REQ_DELETE:Int = 5n;
    public static val REQ_KEY_SET:Int = 6n;
    
	public val transactionId:Long;
    public val requestType:Int;

    public var inKey:Any;
	public var inValue:Any;
    
    public var completed:Boolean;
    public var outValue:Any;
    public var outKeySet:HashSet[Any];
    public var outException:Exception;
    

    public val replicaResponse:ArrayList[Any];
    public var replicas:HashSet[Long];
    public var lateReplicas:HashSet[Long];
    public val responseLock:SimpleLatch;
    
    public val lock:SimpleLatch;
    
    
    public def this(transId:Long, reqType:Int) {
    	this.transactionId = transId;
    	this.requestType = reqType;
    	this.lock = new SimpleLatch();
    	this.responseLock = new SimpleLatch();
    	this.replicaResponse = new ArrayList[Any]();
    }
    
    public def setResponseReplicas(replicas:HashSet[Long]) {
    	this.replicas = replicas;
    	this.lateReplicas = replicas.clone();
    }
    
    public def addReplicaResponse(output:Any, replicaPlaceId:Long) {
    	if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] adding response ...");
    	try {
    		responseLock.lock();
    		
    		if (completed)
        		return;
    		
    		replicaResponse.add(output);
    		lateReplicas.remove(replicaPlaceId);
    		if (lateReplicas.size() == 0) {
    			completed = true;
    			outValue = replicaResponse.get(0);
    		}
    	}
    	finally {
    		responseLock.unlock();
    	}
    	if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] adding response completed ...");
    }
    
    public def findDeadReplica():Long {    	
    	var result:Long = -1;
    	try {
    		responseLock.lock();
    		if (lateReplicas != null) {
    			for (pId in lateReplicas){
    				if (Place(pId).isDead()) {
    					result = pId;
    					break;
    				}
    			}
    		}
    	}
    	finally{
    		responseLock.unlock();
    	}
    	return result;
    }
    
    public def completeFailedRequest(outputException:Exception) {
    	try{
    		responseLock.lock();
    		completed = true;
    		outException = outputException;
    	}finally {
    		responseLock.unlock();
    	}
    }
    
    
    public def isSuccessful() = completed && outException == null;
    
    public def toString():String {
    	var str:String = "";
        str += "<request  transactionId="+transactionId+"  type="+typeDesc(requestType)+">\n";        
        str += "     <input  key="+(inKey)+"/>\n";
        str += "     <input  value="+(inValue)+"/>\n";
        if (outValue != null)
        	str += "     <output  oldValue="+(outValue)+"/>\n";
        if (outKeySet != null)
        	str += "     <output  keySet="+(outKeySet)+"/>\n";
        if (outException != null)
        	str += "     <output  exception="+(outException)+"/>\n";
        str += "</request>\n";		
    	return str;
    }
    
    private def typeDesc(typeId:Int):String {
    	switch(typeId){
    		case REQ_COMMIT: return "Commit";
    		case REQ_ABORT: return "Abort";
    		case REQ_GET: return "Get";
    		case REQ_PUT: return "Put";
    		case REQ_DELETE: return "Delete";
    		case REQ_KEY_SET: return "KeySet";
    	}
    	return null;
    }
}