package x10.util.resilient.map.impl;

import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;

public class MapRequest {
	
	private val moduleName = "MapRequest";
	public static val VERBOSE = Utils.getEnvLong("MAP_REQ_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
	public val id:Long;
	private static val idSequence = new AtomicLong();
	
	
	public static val REQ_COMMIT:Int = 1n;
	public static val REQ_ABORT:Int = 2n;
	public static val REQ_GET:Int = 3n;
    public static val REQ_PUT:Int = 4n;
    public static val REQ_DELETE:Int = 5n;
    
	public static val CONFIRM_COMMIT:Int = 1n;
	public static val CANCELL_COMMIT:Int = 2n;
	public static val CONFIRMATION_SENT:Int = 3n;
    
    public val mapName:String;
	public val timeoutMillis:Long;
	public val transactionId:Long;
    public val requestType:Int;

    public var inKey:Any;
	public var inValue:Any;
    
    public var commitStatus:Int;
    public var completed:Boolean = false;
    public var outValue:Any;
    public var outKeySet:HashSet[Any];
    public var outException:Exception;
    

    public val replicaResponse:ArrayList[Any];
    public var replicas:HashSet[Long];
	//public var replicasVotedToCommit:HashSet[Long];
    private var lateReplicas:HashSet[Long];
    public val responseLock:SimpleLatch;
    
    public val lock:SimpleLatch;
    
    
    public def this(transId:Long, reqType:Int, mapName:String, timeoutMillis:Long) {
    	this.id = idSequence.incrementAndGet();
    	this.transactionId = transId;
    	this.requestType = reqType;
    	this.mapName = mapName;
    	this.timeoutMillis = timeoutMillis;
    	this.lock = new SimpleLatch();
    	this.responseLock = new SimpleLatch();
    	this.replicaResponse = new ArrayList[Any]();
    }
    
    public def setResponseReplicas(replicas:HashSet[Long]) {
    	this.replicas = replicas;
    	this.lateReplicas = replicas.clone();
    }
    
    public def addReplicaResponse(output:Any, exception:Exception, replicaPlaceId:Long) {
    	if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] adding response for request === " + this.toString());
    	try {
    		responseLock.lock();
    		
    		if (completed) { //ignore late responses
    			if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] RESPONSE IGNORED  for request ==== " + this.toString());
        		return;
    		}
    		
    		outException = exception;
    		if (exception != null)
    			replicaResponse.add(output);
    		lateReplicas.remove(replicaPlaceId);
    		if (lateReplicas.size() == 0) {
    			completed = true;
    			if (outException == null)
    				outValue = replicaResponse.get(0);
    		}
    	}
    	finally {
    		responseLock.unlock();
    	}
    	if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] adding response completed ...");
    }
    
    
    public def commitVote(vote:Long, replicaPlaceId:Long) {
    	if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] adding vote response ["+vote+"] ...");
    	try {
    		responseLock.lock();
    		
    		if (completed) {
    			if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] VOTE IGNORED ...");
        		return;
    		}
    		
    		replicaResponse.add(vote);
    		lateReplicas.remove(replicaPlaceId);
    		if (lateReplicas.size() == 0) {
    			commitStatus = CONFIRM_COMMIT;
    			    					
    			for (resp in replicaResponse) {
    				if (resp == 0) {
    					commitStatus = CANCELL_COMMIT;
    					break;
    				}
    			}
    			Utils.console(moduleName, "Received all votes for trans ["+transactionId+"] - decision is "+commitStatus+" ...");
				
    			if (VERBOSE) {
    				if (commitStatus == CONFIRM_COMMIT)
    					Utils.console(moduleName, "Received all votes for trans ["+transactionId+"] - decision is COMMIT ...");
    				else
    					Utils.console(moduleName, "Received all votes for trans ["+transactionId+"] - decision is ABORT ...");
    			}
    		}
    		else {
    			var str:String = "";
    		    for (x in lateReplicas)
    		    	str += x + ",";
    			if (VERBOSE) Utils.console(moduleName, "waiting for votes from places ["+str+"] ");
    		}
    	}
    	finally {
    		responseLock.unlock();
    	}
    	if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] adding vote response completed ...");
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
    		if (VERBOSE) Utils.console(moduleName, "Completing failed request: " + this.toString() + "  Reason: " + outputException);
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
    	str += "<request Id["+id+"] type["+typeDesc(requestType)+ "]>\n";
        /*
        str += "<request  transactionId="+transactionId+"  type="+typeDesc(requestType)+ "  key=" + inKey + " \\>\n";
        if (outValue != null)
        	str += "     <output  oldValue="+(outValue)+"/>\n";
        if (outKeySet != null)
        	str += "     <output  keySet="+(outKeySet)+"/>\n";
        if (outException != null)
        	str += "     <output  exception="+(outException)+"/>\n";
        	
        str += "</request>\n";
        */		
    	return str;
    }
    
    public static def typeDesc(typeId:Int):String {
    	switch(typeId){
    		case REQ_COMMIT: return "Commit-"+typeId;
    		case REQ_ABORT: return "Abort-"+typeId;
    		case REQ_GET: return "Get-"+typeId;
    		case REQ_PUT: return "Put-"+typeId;
    		case REQ_DELETE: return "Delete-"+typeId;
    	}
    	return "UnknowReqType-"+typeId;
    }
}