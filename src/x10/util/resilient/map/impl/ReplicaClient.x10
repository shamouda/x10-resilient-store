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
import x10.util.resilient.map.exception.InvalidDataStoreException;
import x10.util.Team;
import x10.xrx.Runtime;
import x10.compiler.Ifdef;
/**
 * The ReplicaClient receives MapRequests from ResilientMapImpl an execute them on relevant Replicas
 **/
public class ReplicaClient {
    private val moduleName = "ReplicaClient";
    public static val REPLICA_CLIENT_SLEEP = Utils.getEnvLong("REPLICA_CLIENT_SLEEP", 100);    
    public static val REQUEST_TIMEOUT = Utils.getEnvLong("REQUEST_TIMEOUT", 100) ;
    
    /*Copy of the parition table*/
    private var partitionTable:PartitionTable;

    private val pendingRequests:ArrayList[MapRequest];
    
    /* Maps the transaction id to the replicas that handle its requests.
     * needed for commit and abort*/
    private val transReplicas:HashMap[Long,HashSet[Long]];
    
    private val notifiedDeadReplicas = new ArrayList[Long]();
    private var lastDeadPlaceNotificationTime:Long;
    
    
    private val lock:SimpleLatch;
    
    private var timerOn:Boolean = false;
    
    private var valid:Boolean = true;
    
    public def this(partitionTable:PartitionTable) {
        this.partitionTable = partitionTable;
        pendingRequests = new ArrayList[MapRequest]();
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
    
    public def asyncExecuteRequest(request:MapRequest) {
    	if (!valid) {
    		@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "asyncExecuteRequest: Invalid ReplicaClient ..."); }
    		return;
    	}
    	
        if (Utils.KILL_PLACE_POINT == Utils.POINT_BEGIN_ASYNC_EXEC_REQUEST)
            Utils.asyncKillPlace();
        
        switch(request.requestType) {
            case MapRequest.REQ_GET:    asyncExecuteSingleKeyRequest(request); break;
            case MapRequest.REQ_PUT:    asyncExecuteSingleKeyRequest(request); break;
            case MapRequest.REQ_DELETE: asyncExecuteSingleKeyRequest(request); break;
            case MapRequest.REQ_PREPARE_ONLY: asyncExecutePrepareCommit(request); break;
            case MapRequest.REQ_PREPARE_AND_COMMIT: asyncExecutePrepareCommit(request); break;
            case MapRequest.REQ_CONFIRM_COMMIT: asyncExecuteConfirmCommit(request); break;
            case MapRequest.REQ_ABORT:          asyncExecuteAbort(request); break;
        }        
    }
    
    /**
     * Start Get/Put/Delete request
     **/
    private def asyncExecuteSingleKeyRequest(request:MapRequest) {
        val key = request.inKey;
        val repInfo = partitionTable.getKeyReplicas(key);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Key["+key+"] "+repInfo.toString()); }
        val submit = addPendingRequest(request, repInfo.replicas);
        if (submit)
            submitSingleKeyRequest(request, repInfo.replicas, repInfo.partitionId);
        @Ifdef("__DS_DEBUG__") {
            if (!submit) Utils.console(moduleName, "Request Held until partition table is updated: " + request.toString());
        }
    }
    
    /**
     * Start prepare commit request
     **/
    private def asyncExecutePrepareCommit(request:MapRequest) {
        val transId = request.transactionId;
        var replicas:HashSet[Long] = request.replicas;
        if (request.replicas == null)
        	replicas = getTransactionReplicas(transId);        
        val submit = addPendingRequest(request, replicas);
        if (submit)
            submitAsyncPrepareCommit(request, replicas);
        else {
            val deadPlaceId = Utils.getDeadReplicas(replicas).iterator().next(); 
            request.completeRequest(new DeadPlaceException(Place(deadPlaceId)));
        }
    }
    
    /**
     * Start confirm commit request.
     * If some replicas are dead, ignore them and commit at the other active replicas.
     * If all replicas are dead, then throw an exception and mark the DataStore as invalid 
     **/
    private def asyncExecuteConfirmCommit(request:MapRequest) {
        val replicas = request.replicas;
        request.setReplicationInfo(replicas);
        val allReplicasActive = addPendingRequest(request, replicas);
        submitAsyncConfirmCommit(request, replicas);
    }
    
    /**
     * Start abort request
     **/
    private def asyncExecuteAbort(request:MapRequest) {
        val transId = request.transactionId;
        var replicas:HashSet[Long] = request.replicas;
        if (request.replicas == null)
        	replicas = getTransactionReplicas(transId);        
        if (replicas == null){
        	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "abort successfully because replicas are null for request: " + request.toString()); }
            //transaction was not submitted to any replica
            request.completeRequest(null);
            return;
        }
        
        val allReplicasActive = addPendingRequest(request, replicas); 
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "allReplicasActive= " + allReplicasActive); }        
        submitAsyncExecuteAbort(request, replicas);
    }
    
    /************************  Functions to send requests to Replicas  *****************************/
    private def submitSingleKeyRequest(request:MapRequest, replicas:HashSet[Long], partitionId:Long) {
        val key = request.inKey;
        val value = request.inValue;
        val mapName = request.mapName;
        val requestType = request.requestType;
        val transId = request.transactionId;
        val gr = GlobalRef[MapRequest](request);
        val clientId = here.id;
        request.setReplicationInfo(replicas);
        
        var exception:Exception = null;
        for (placeId in replicas) {
        	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "SubmittingToPlace["+Place(placeId)+"] request: " + request.toString() ); } 
            try{
                at (Place(placeId)) async {
                	//Console.OUT.println("SubmittingToPlace - Reached " + here + "  transId["+transId+"] ");
                    DataStore.getInstance().getReplica().submitSingleKeyRequest(mapName, clientId, partitionId, transId, requestType, key, value, replicas, gr);
                }
            }
            catch (ex:Exception) {
                exception = ex;
                @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "exception while submitting ["+ex.getMessage()+"]  request: " + request.toString() ); }
                break;
            }
        }
        if (exception != null)
            request.completeRequest(exception);
    }
    
    private def submitAsyncPrepareCommit(request:MapRequest, replicas:HashSet[Long]) {
    	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Submitting ReadyToCommit? recovery["+request.commitRecovery+"] for request: " + request.toString()); }    
        val requestType = request.requestType;
        val transId = request.transactionId;
        val mapName = request.mapName;
        val recoveryCommit = request.commitRecovery; 
        val gr = GlobalRef[MapRequest](request);
        val clientId = here.id;
        request.setReplicationInfo(replicas);
        
        var exception:Exception = null;
        for (placeId in replicas) {
            try{
                at (Place(placeId)) async {
                    DataStore.getInstance().getReplica().prepareCommit(clientId, transId, recoveryCommit, gr);
                }
            }
            catch (ex:Exception) {
                exception = ex;
                break;
            }
        }
        if (exception != null)
            request.completeRequest(exception);
    }
    
    private def submitAsyncConfirmCommit(request:MapRequest, replicas:HashSet[Long]) {
    	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Submitting ConfirmCommit for request: " + request.toString()); }    
        val transId = request.transactionId;
        val mapName = request.mapName;
        val gr = GlobalRef[MapRequest](request);
        
        var allReplicasDied:Boolean = true;
        var exception:Exception = null;
        for (placeId in replicas) {
            try{
                if (!Place(placeId).isDead()) {
                    allReplicasDied = false;
                    at (Place(placeId)) async {
                        DataStore.getInstance().getReplica().commitNoResponse(transId, gr);
                    }
                }
            }
            catch (ex:Exception) {
                exception = ex;
            }
        }
        
        if (allReplicasDied) {
            valid = false;
            DataStore.getInstance().invalidate();
            request.completeRequest(new InvalidDataStoreException());
        }
        else{
          //Ignore exceptions
            request.completeRequest(null);
        }
    }
    
    private def submitAsyncExecuteAbort(request:MapRequest, replicas:HashSet[Long]) {
    	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Submitting request: " + request.toString()); }        
        val transId = request.transactionId;
        val gr = GlobalRef[MapRequest](request);
        request.setReplicationInfo(replicas);
        
        var allReplicasDied:Boolean = true;
        var exception:Exception = null;
        for (placeId in replicas) {
            try{
            	if (!Place(placeId).isDead()) {
            	    allReplicasDied = false;
            		at (Place(placeId)) async {
                    	DataStore.getInstance().getReplica().abortNoResponse(transId, gr);
                	}
            	}
            }
            catch (ex:Exception) {
                exception = ex;
            }
        }
        
        if (allReplicasDied) {
            valid = false;
            DataStore.getInstance().invalidate();
            request.completeRequest(new InvalidDataStoreException());
        }
        else{
          //Ignore exceptions
            request.completeRequest(null);
        }
    }
    
    /************************  Functions to Add and Monitor Requests  *****************************/
    
    /**
     * Returns true if all replicas are active
     * */
    private def addPendingRequest(req:MapRequest, replicas:HashSet[Long]):Boolean {
    	if (!valid)
    		return false;
    	
        var result:Boolean = true;
        try{
            lock.lock();            
            
            req.startTimeMillis = Timer.milliTime();
            
            pendingRequests.add(req);
           
            val deadPlacesFound = notifyDeadPlaces(replicas);
            
            if (!deadPlacesFound) {
                if (! (req.requestType == MapRequest.REQ_PREPARE_AND_COMMIT || req.requestType == MapRequest.REQ_PREPARE_ONLY || req.requestType == MapRequest.REQ_CONFIRM_COMMIT || req.requestType == MapRequest.REQ_ABORT)) {
                    //append to the transaction replicas (needed for commit)//
                    var set:HashSet[Long] = transReplicas.getOrElse(req.transactionId,new HashSet[Long]());
                    set.addAll(replicas);
                    transReplicas.put(req.transactionId,set);
                }
            }
            else{
                req.requestStatus = MapRequest.STATUS_PENDING_MIGRATION;
                req.oldPartitionTableVersion = partitionTable.getVersion();  // don't submit untill the partition table is updated
                result = false;            
            }
            
            if (!timerOn){
                timerOn = true;
                async checkPendingTransactions();
            }
        }
        catch(ex:Exception) {
        	ex.printStackTrace();
        	valid = false;
        	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Exception while adding request " + req.toString() + "  exception:" + ex.getMessage()); }
        }
        finally{
            lock.unlock();
        }
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Added Pending Request " + req.toString()); }
        return result;
    }
    
    /**
     * Monitor pending requests:
     *  - release completed requests
     *  - release requests waiting for dead places
     *  - release requests exceeding their time out limit
     *  - check for readyToCommit transactions and issue their confirmCommit request 
     *  - check for pendingMigration requests, and issues them after migration is complete
     **/
    private def checkPendingTransactions() {
        Runtime.increaseParallelism();
        while (timerOn && valid) {            
            //requests that were pending until migration completes
            val resubmitList = new ArrayList[MapRequest]();
            
            System.threadSleep(REPLICA_CLIENT_SLEEP);
            try{
                lock.lock();
                var i:Long;
                @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "checkPendingTransactions: new iteration ..."); }
                
                for (i = 0; i< pendingRequests.size(); i++){
                    val mapReq = pendingRequests.get(i);
                    var checkTimeout:Boolean = true;
                    
                    if (mapReq.requestStatus == MapRequest.STATUS_COMPLETED) {
                        pendingRequests.removeAt(i--);
                        checkTimeout = false;
                    }
                    else if (mapReq.requestStatus == MapRequest.STATUS_PENDING_MIGRATION && 
                            mapReq.oldPartitionTableVersion != partitionTable.getVersion()) {
                        mapReq.requestStatus = MapRequest.STATUS_STARTED;
                        resubmitList.add(mapReq);
                        pendingRequests.removeAt(i--);
                        checkTimeout = false;
                    }
                    else if (mapReq.requestType == MapRequest.REQ_PREPARE_AND_COMMIT) {
                        if (mapReq.commitStatus == MapRequest.CONFIRM_COMMIT) {
                            mapReq.requestType = MapRequest.REQ_CONFIRM_COMMIT;
                            resubmitList.add(mapReq);
                            pendingRequests.removeAt(i--);
                            checkTimeout = false;
                        }
                        else if (mapReq.commitStatus == MapRequest.CANCELL_COMMIT) {
                            mapReq.completeRequest(new CommitVotingFailedException());    
                            pendingRequests.removeAt(i--);
                            checkTimeout = false;
                        }
                    }                    
                    if (checkTimeout) {
                        if (Timer.milliTime() - mapReq.startTimeMillis > REQUEST_TIMEOUT) {
                            mapReq.completeRequest(new RequestTimeoutException());    
                            pendingRequests.removeAt(i);
                            i--;
                        }
                        else if (mapReq.lateReplicas != null) {
                            val deadReplicas = mapReq.getRequestDeadReplicas();
                            if (deadReplicas.size() != 0) {
                                val deadPlaceId = deadReplicas.iterator().next(); 
                                mapReq.completeRequest(new DeadPlaceException(Place(deadPlaceId)));       
                                pendingRequests.removeAt(i--);
                            }
                        }
                    }
                }//for loop on pendingRequests
                
                if (pendingRequests.size() == 0){
                    timerOn = false;
                }
            }
            finally{
                lock.unlock();
            }
            
            if (valid) {
                //resubmit the requests that were pending on migration or on commit confirmation
                for (req in resubmitList) {
                    asyncExecuteRequest(req);
                }
            }
            @Ifdef("__DS_DEBUG__") {
                if (!valid) Utils.console(moduleName, "checkPendingTransactions: Invalid ReplicaClient ...");
            }
        }
        Runtime.decreaseParallelism(1n);
    }
    
    /**
     * TODO: move this function to DataStore because we need the leader
     * 
     * Checks if any of the provided replicas is dead, and notifies the master place
     * Returns true if any replica is dead
     **/
    private def notifyDeadPlaces(replicas:HashSet[Long]):Boolean {
        var result:Boolean = true;
        val deadReplicas = Utils.getDeadReplicas(replicas); 
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "notifyDeadPlaces: deadReplicas count is ["+deadReplicas.size()+"] ..."); }
        if (deadReplicas.size() != 0) {
            val notifyList = new HashSet[Long]();
            for (newDead in deadReplicas) {
                if (!notifiedDeadReplicas.contains(newDead)) {
                    notifyList.add(newDead);
                    notifiedDeadReplicas.add(newDead);
                }
            }            
            if (notifyList.size() > 0 || 
                Timer.milliTime() - lastDeadPlaceNotificationTime > Utils.TIMEOUT_TO_RENOTIFY_LEADER) {
                lastDeadPlaceNotificationTime = Timer.milliTime();
                DataStore.getInstance().clientNotifyDeadPlaces(notifyList);  
            }
            else {
            	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Dead places already notified ..."); }
            }
        }
        else
            result = false;
        return result;
    }
}