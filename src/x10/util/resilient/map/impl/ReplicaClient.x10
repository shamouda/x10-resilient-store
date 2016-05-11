package x10.util.resilient.map.impl;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.partition.PartitionTable;
import x10.util.resilient.map.partition.PartitionReplicas;
import x10.util.resilient.map.DataStore;
import x10.util.resilient.map.exception.RequestTimeoutException;
import x10.util.resilient.map.exception.CommitVotingFailedException;


public class ReplicaClient {
    private val moduleName = "ReplicaClient";
    public static val VERBOSE = Utils.getEnvLong("REPL_MNGR_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    
    
    //this is the class that communicates with the replicas
    private var partitionTable:PartitionTable;
    private var partitionTableVersion:Long = 1;

    private val pendingRequests:ArrayList[RequestState];
    
    /* Maps the transaction id to the replicas that handle its requests.
     * needed for commit and abort*/
    private val transReplicas:HashMap[Long,HashSet[Long]];
    
    private val notifiedDeadReplicas = new ArrayList[Long]();
    
    private val lock:SimpleLatch;
    
    private var timerOn:Boolean = false;
    
    public def this(partitionTable:PartitionTable) {
        this.partitionTable = partitionTable;
        pendingRequests = new ArrayList[RequestState]();
        transReplicas = new HashMap[Long,HashSet[Long]]();
        lock = new SimpleLatch(); 
    }
    
    public def getTransactionReplicas(transactionId:Long):HashSet[Long] {
        try {
            lock.lock();
            return transReplicas.getOrElse(transactionId,null);
        }
        finally {
            lock.unlock();
        }
    }

    public def updatePartitionTable(p:PartitionTable) {
        try{
            lock.lock();
            partitionTableVersion++;
            partitionTable = p;
        }
        finally {
            lock.unlock();
        }
    }   
    
    /**
     * Returns true if all replicas are active (which means the request is valid for immediate submission)
     * */
    private def asyncWaitForResponse(req:MapRequest, replicas:HashSet[Long], partitionId:Long):Boolean {
    	var result:Boolean = true;
        try{
            lock.lock();            
            
            pendingRequests.add(new RequestState(req, Timer.milliTime()));
            
            val deadReplicas = Utils.getDeadReplicas(replicas); 
            if (deadReplicas.size() == 0) {            	
            	req.setReplicationInfo(replicas, partitionId);
            		
            	if (! (req.requestType == MapRequest.REQ_COMMIT || req.requestType == MapRequest.REQ_ABORT)) {
            		//append to the transaction replicas (needed for commit)//
            		var set:HashSet[Long] = transReplicas.getOrElse(req.transactionId,null);
               		if (set == null) {
               			set = new HashSet[Long]();
               			transReplicas.put(req.transactionId,set);
               		}
               		set.addAll(replicas);
            	}
            }
            else{
            	req.requestStatus = MapRequest.STATUS_PENDING_SUBMIT;
            	req.oldPartitionTableVersion = partitionTableVersion;  // don't submit untill the partition table is updated
            	result = false;
               
            	val newDeadPlaces = excludeNotifiedDeadPlaces(deadReplicas);
            	async DataStore.getInstance().clientNotifyDeadPlaces(newDeadPlaces);            	
            }
            
            if (!timerOn){
                timerOn = true;
                async checkPendingTransactions();
            }
        }
        finally{
            lock.unlock();
        }
        if (VERBOSE) Utils.console(moduleName, "Added Pending Request " + req.toString());
        return result;
    }
    
    public def asyncExecuteRequest(request:MapRequest) {
    	if (Utils.KILL_PLACE_POINT == Utils.POINT_BEGIN_ASYNC_EXEC_REQUEST)
    		Utils.asyncKillPlace();
    	
        switch(request.requestType) {
            case MapRequest.REQ_GET: 
                asyncExecuteSingleKeyRequest(request); break;
            case MapRequest.REQ_PUT: asyncExecuteSingleKeyRequest(request); break;
            case MapRequest.REQ_DELETE: asyncExecuteSingleKeyRequest(request); break;
            case MapRequest.REQ_COMMIT: asyncExecuteCommit(request); break;
            case MapRequest.REQ_ABORT: asyncExecuteAbort(request); break;
        }        
    }
    
    private def asyncExecuteSingleKeyRequest(request:MapRequest) {
        val key = request.inKey;        
        val repInfo = partitionTable.getKeyReplicas(key);
        val submit = asyncWaitForResponse(request, repInfo.replicas, repInfo.partitionId);
        if (submit)
        	submitSingleKeyRequest(request);
        else
        	 if (VERBOSE) Utils.console(moduleName, "Request Held until partition table is updated: " + request.toString());    
    }
    
    private def submitSingleKeyRequest(request:MapRequest) {
    	if (VERBOSE) Utils.console(moduleName, "Submitting request: " + request.toString());    
    	val key = request.inKey;
        val value = request.inValue;
        val mapName = request.mapName;
        val requestType = request.requestType;
        val transId = request.transactionId;
        val partitionId = request.partitionId;
        val gr = GlobalRef[MapRequest](request);
        
    	for (placeId in request.replicas) {
            try{
                at (Place(placeId)) async {
                    DataStore.getInstance().getReplica().submitSingleKeyRequest(mapName, here.id, partitionId, transId, requestType, key, value, gr);
                }
            }
            catch (ex:Exception) {
                request.completeFailedRequest(ex);
                break;
            }
        }
    }
    
    private def asyncExecuteCommit(request:MapRequest) {           
        val transId = request.transactionId;
        val replicas = getTransactionReplicas(transId);
        val submit = asyncWaitForResponse(request, replicas, -1);
        if (submit)
        	submitAsyncExecuteCommit(request);
        else
        	if (VERBOSE) Utils.console(moduleName, "Request Held until partition table is updated: " + request.toString()); 
    }
    
    
    private def submitAsyncExecuteCommit(request:MapRequest) {
        if (VERBOSE) Utils.console(moduleName, "Submitting ReadyToCommit?  for request: " + request.toString());    
        val requestType = request.requestType;
        val transId = request.transactionId;
        val mapName = request.mapName;
        val gr = GlobalRef[MapRequest](request);
        
        for (placeId in request.replicas) {
            try{
                at (Place(placeId)) async {
                    DataStore.getInstance().getReplica().submitReadyToCommit(mapName, transId, gr);
                }
            }
            catch (ex:Exception) {
                request.completeFailedRequest(ex);
                break;
            }
        }    
    }
    

    private def asyncExecuteConfirmCommit(request:MapRequest) {
        if (VERBOSE) Utils.console(moduleName, "Submitting ConfirmCommit for request: " + request.toString());    
        val requestType = request.requestType;
        val transId = request.transactionId;
        val mapName = request.mapName;
        val replicas = request.replicas;
        request.setReplicationInfo(request.replicas, request.partitionId);

        val gr = GlobalRef[MapRequest](request);
        
        for (placeId in replicas) {
            try{
                at (Place(placeId)) async {
                    DataStore.getInstance().getReplica().submitConfirmCommit(mapName, transId, gr);
                }
            }
            catch (ex:Exception) {
                //should only be a dead place exception
                //TODO: handle commit exception
                ex.printStackTrace();
            }
        }    
        
    }
    
    private def asyncExecuteAbort(request:MapRequest) {
    	val transId = request.transactionId;
        val replicas = getTransactionReplicas(transId);       
        val submit = asyncWaitForResponse(request, replicas, -1);
        if (submit)
        	submitAsyncExecuteAbort(request);
        else
        	if (VERBOSE) Utils.console(moduleName, "Request Held until partition table is updated: " + request.toString()); 
        
    }
    
    
    
    private def submitAsyncExecuteAbort(request:MapRequest) {
        if (VERBOSE) Utils.console(moduleName, "Submitting request: " + request.toString());        
        val transId = request.transactionId;
        val gr = GlobalRef[MapRequest](request);
        
        var exception:Exception = null;
        for (placeId in request.replicas) {
            try{
                at (Place(placeId)) async {
                    DataStore.getInstance().getReplica().submitAbort(transId, gr);
                }
            }
            catch (ex:Exception) {
                exception = ex;
                ex.printStackTrace();
            }
        }
        request.completeFailedRequest(exception);
    }
    
    
    private def checkPendingTransactions() {
        while (timerOn) {
        	val resubmitList = new ArrayList[MapRequest]();
            System.threadSleep(10);
            try{
                lock.lock();
                var i:Long;
                if (VERBOSE) Utils.console(moduleName, "Check pending transactions new iteration ...");
                
                for (i = 0; i< pendingRequests.size(); i++){
                    val curReq = pendingRequests.get(i);
                    
                    val mapReq = curReq.req;
                    if (mapReq.requestStatus == MapRequest.STATUS_COMPLETED) {
                        mapReq.lock.release();
                        pendingRequests.removeAt(i);
                        i--;
                    }
                    else if (mapReq.requestType == MapRequest.STATUS_PENDING_SUBMIT && mapReq.oldPartitionTableVersion != partitionTableVersion) {
                    	mapReq.requestStatus = MapRequest.STATUS_STARTED;
                    	resubmitList.add(mapReq);
                    	pendingRequests.removeAt(i);
                        i--;
                    }
                    else if (mapReq.requestType == MapRequest.REQ_COMMIT) {
                        if (mapReq.commitStatus == MapRequest.CONFIRM_COMMIT) {
                            mapReq.commitStatus = MapRequest.CONFIRMATION_SENT; // to avoid sending another confirmation
                            asyncExecuteConfirmCommit(mapReq);
                        }
                        else if (mapReq.commitStatus == MapRequest.CANCELL_COMMIT) {
                            mapReq.completeFailedRequest(new CommitVotingFailedException());    
                            mapReq.lock.release();
                            pendingRequests.removeAt(i);
                            i--;
                            //an abort request will be issued because of this exception
                        }
                        //TODO: do we need a time out here?
                    }
                    else if (Timer.milliTime() - curReq.startTimeMillis > curReq.timeoutMillis()) {
                        //TODO: because commit requires multiple interaction- we need to save the last interaction time
                        mapReq.completeFailedRequest(new RequestTimeoutException());    
                        mapReq.lock.release();
                        pendingRequests.removeAt(i);
                        i--;
                    }
                    else {
                        val pId = mapReq.findDeadReplica();
                        if (pId != -1) {
                            mapReq.completeFailedRequest(new DeadPlaceException(Place(pId)));                            
                            mapReq.lock.release();
                            pendingRequests.removeAt(i);
                            i--;
                        }
                    }
                }
                
                if (pendingRequests.size() == 0){
                    timerOn = false;
                }
            }
            finally{
                lock.unlock();
            }
            
            //resubmit the requests that were pending on updating the partition table
            for (req in resubmitList) {
            	asyncExecuteRequest(req);
            }
        }
    }
    
    private def excludeNotifiedDeadPlaces(deadReplicas:HashSet[Long]):HashSet[Long] {    	
    	val leaderList = new HashSet[Long]();
    	for (newDead in deadReplicas) {
    		if (!notifiedDeadReplicas.contains(newDead))
    			leaderList.add(newDead);
    	}    		
    	return leaderList;
    }
}

class RequestState {
    public val req:MapRequest;
    public val startTimeMillis:Long;
    public var lastActionTimeMillis:Long;
    public def this (req:MapRequest, startTimeMillis:Long) {
        this.req = req;
        this.startTimeMillis = startTimeMillis;
        this.lastActionTimeMillis = startTimeMillis;
    }
    public def timeoutMillis() = req.timeoutMillis;
}