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
    
    public val mapName:String;
    public val timeoutMillis:Long;
    public val transactionId:Long;
    public var requestType:Int;    
    
    //main status//
    public var requestStatus:Int = STATUS_STARTED;
    
    //sub status//
    public var commitStatus:Int = UNUSED_COMMIT;
    public var oldPartitionTableVersion:Long;

    //user inputs and outputs
    public var inKey:Any;
    public var inValue:Any;
    public var outValue:Any;
    public var outKeySet:HashSet[Any];
    public var outException:Exception;

    /*The values received from replicas*/
    public val replicaResponse:ArrayList[Any];
    
    /*All replicas participating in the request*/
    public var replicas:HashSet[Long];
    
    /*Replicas have not completed request processing*/
    public var lateReplicas:HashSet[Long];
    
    /* Lock used by ResilientMapImpl to wait for the completion of the request*/
    public val lock:SimpleLatch;
    
    /*Lock used to serialize accesses from different replicas*/
    public val responseLock:SimpleLatch;
    
    /*Processing start time. It is a var because the request can be resubmitted with a new time*/
    public var startTimeMillis:Long;
    
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
    
    public def setReplicationInfo(replicas:HashSet[Long]) {
        this.replicas = replicas;
        if (replicas != null)
            this.lateReplicas = replicas.clone();
    }
    
    public def addReplicaResponse(output:Any, exception:Exception, replicaPlaceId:Long) {
        if (VERBOSE) Utils.console(moduleName, "TransId["+transactionId+"] From ["+replicaPlaceId+"] adding response for request === " + this.toString()  + " ..... output["+output+"] exception["+exception+"] ");
        try {
            responseLock.lock();
            
            if (requestStatus == STATUS_COMPLETED) { //ignore late responses
                if (VERBOSE) Utils.console(moduleName, "TransId["+transactionId+"] From ["+replicaPlaceId+"] RESPONSE IGNORED  for request ==== " + this.toString());
                return;
            }
            
            lateReplicas.remove(replicaPlaceId);
            
            outException = exception;
            
            if (exception == null)
                replicaResponse.add(output);
            
            if (lateReplicas.size() == 0) {
                requestStatus = STATUS_COMPLETED;
                if (outException == null)
                    outValue = replicaResponse.get(0);
            }
        }
        finally {
            responseLock.unlock();
        }
        //if (VERBOSE) Utils.console(moduleName, "TransId["+transactionId+"] From ["+replicaPlaceId+"] adding response completed ...");
    }
    
    
    public def commitVote(vote:Long, replicaPlaceId:Long) {
        if (VERBOSE) Utils.console(moduleName, "TransId["+transactionId+"] From ["+replicaPlaceId+"] adding vote response ["+vote+"] ...");
        try {
            responseLock.lock();
            
            if (requestStatus == STATUS_COMPLETED) {
                if (VERBOSE) Utils.console(moduleName, "TransId["+transactionId+"] From ["+replicaPlaceId+"] VOTE IGNORED ...");
                return;
            }
            
            replicaResponse.add(vote);
            lateReplicas.remove(replicaPlaceId);
            
            if (vote == 0)
                commitStatus = CANCELL_COMMIT;
            else if (lateReplicas.size() == 0 && commitStatus != CANCELL_COMMIT) //vote==1
                commitStatus = CONFIRM_COMMIT;
            

            if (VERBOSE) {
                var str:String = "";
                for (x in lateReplicas)
                    str += x + ",";
                Utils.console(moduleName, "TransId["+transactionId+"] Waiting for votes from places ["+str+"] commitStatus is["+commitStatusDesc(commitStatus)+"] ");
            }
        }
        finally {
            responseLock.unlock();
        }
        if (VERBOSE) Utils.console(moduleName, "From ["+replicaPlaceId+"] adding vote response completed ...");
    }
    
    public def getRequestDeadReplicas():HashSet[Long] {        
        val result = new HashSet[Long]();
        try {
            responseLock.lock();
            if (lateReplicas != null) {
                for (pId in lateReplicas){
                    if (Place(pId).isDead()) {
                        result.add(pId);                        
                    }
                }
            }
        }
        finally{
            responseLock.unlock();
        }
        return result;
    }
    
    public def completeRequest(outputException:Exception) {
        try{
            if (VERBOSE) Utils.console(moduleName, "Completing request: " + this.toString() + "  Exception: " + outputException);
            responseLock.lock();
            requestStatus = STATUS_COMPLETED;
            outException = outputException;
        }finally {
            responseLock.unlock();
        }
    }
    
    public def isSuccessful() = requestStatus == STATUS_COMPLETED && outException == null;
    
    public def toString():String {
        var str:String = "";
        str += "<request Id["+id+"] type["+typeDesc(requestType)+ "]>";
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
            case REQ_PREPARE_COMMIT: return "PrepareCommit";
            case REQ_COMMIT:          return "Commit";
            case REQ_ABORT:          return "Abort";
            case REQ_GET:              return "Get";
            case REQ_PUT:              return "Put";
            case REQ_DELETE:          return "Delete";
        }
        return "UnknowReqType-"+typeId;
    }
    
    public static def commitStatusDesc(commitStatusId:Int):String {
        switch(commitStatusId){
            case UNUSED_COMMIT:     return "UnusedCommit";
            case CONFIRM_COMMIT:    return "ConfirmCommit";
            case CANCELL_COMMIT:     return "CancelCommit";
            case CONFIRMATION_SENT: return "ConfirmationSent";
        }
        return "UnknownCommitStatus";
    }
    
    
    /******   CONSTANTS  *******/
    //request type//
    public static val REQ_GET:Int = 1n;
    public static val REQ_PUT:Int = 2n;
    public static val REQ_DELETE:Int = 3n;
    public static val REQ_PREPARE_COMMIT:Int = 4n;
    public static val REQ_COMMIT:Int = 5n;
    public static val REQ_ABORT:Int = 6n;
    
    //Two phase commit status//
    public static val UNUSED_COMMIT:Int = 0n;
    public static val CONFIRM_COMMIT:Int = 1n;
    public static val CANCELL_COMMIT:Int = 2n;
    public static val CONFIRMATION_SENT:Int = 3n;
    
    //Request status//
    public static val STATUS_STARTED:Int = 1n;
    public static val STATUS_PENDING_MIGRATION:Int = 2n;
    public static val STATUS_COMPLETED:Int = 10n;
}