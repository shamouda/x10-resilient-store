package x10.util.resilient.map.impl;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.partition.PartitionTable;
import x10.util.resilient.map.partition.PartitionReplicationInfo;
import x10.util.resilient.map.DataStore;
import x10.util.resilient.map.exception.RequestTimeoutException;
import x10.util.resilient.map.exception.CommitVotingFailedException;


public class ReplicaClient {
	private val moduleName = "ReplicaClient";
	public static val VERBOSE = Utils.getEnvLong("REPL_MNGR_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
    //this is the class that communicates with the replicas
    private var partitionTable:PartitionTable;
	
	private val pendingRequests:ArrayList[RequestState];
	
	/* Maps the transaction id to the replicas that handle its requests.
	 * needed for commit and abort*/
	private val transReplicas:HashMap[Long,HashSet[Long]];
	
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
			partitionTable = p;
		}
		finally {
			lock.unlock();
		}
	}
	
	private def asyncWaitForResponse(req:MapRequest, replicas:HashSet[Long]) {
		try{
			lock.lock();
			pendingRequests.add(new RequestState(req, Timer.milliTime()));
			
			if (replicas != null) {
				val transId = req.transactionId;
			
				var set:HashSet[Long] = transReplicas.getOrElse(transId,null);
				if (set == null) {
					set = new HashSet[Long]();
					transReplicas.put(transId,set);
				}
				set.addAll(replicas);
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
	}
	
	public def asyncExecuteRequest(request:MapRequest) {
		if (VERBOSE) Utils.console(moduleName, "Submitting request: " + request.toString());		
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
		val value = request.inValue;
		val mapName = request.mapName;
		val requestType = request.requestType;
		val transId = request.transactionId;
		val repInfo = partitionTable.getKeyReplicas(key);
		request.setResponseReplicas(repInfo.replicas);
		val gr = GlobalRef[MapRequest](request);
		
		asyncWaitForResponse(request, repInfo.replicas);

		for (placeId in repInfo.replicas) {
			try{
				at (Place(placeId)) async {
					DataStore.getInstance().getReplica().submitSingleKeyRequest(mapName, here.id, repInfo.partitionId, transId, requestType, key, value, gr);
				}
			}
			catch (ex:Exception) {
				request.completeFailedRequest(ex);
				break;
			}
		}
	}
	
	private def asyncExecuteCommit(request:MapRequest) {
		if (VERBOSE) Utils.console(moduleName, "Submitting READY for request: " + request.toString());	
		val requestType = request.requestType;
		val transId = request.transactionId;
		val mapName = request.mapName;
		
		val replicas = getTransactionReplicas(transId);
		request.setResponseReplicas(replicas);
		val gr = GlobalRef[MapRequest](request);
		
		asyncWaitForResponse(request, null);
		
		for (placeId in replicas) {
			try{
				at (Place(placeId)) async {
					DataStore.getInstance().getReplica().submitReadyToCommit(transId, gr);
				}
			}
			catch (ex:Exception) {
				request.completeFailedRequest(ex);
				break;
			}
		}	
	}
	

	private def asyncExecuteConfirmCommit(request:MapRequest) {
		if (VERBOSE) Utils.console(moduleName, "Submitting CONFIRM COMMIT for request: " + request.toString());	
		val requestType = request.requestType;
		val transId = request.transactionId;
		val mapName = request.mapName;
		val replicas = request.replicas;
		request.setResponseReplicas(request.replicas);

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
		if (VERBOSE) Utils.console(moduleName, "Submitting request: " + request.toString());		
		val transId = request.transactionId;
		val targetReplicas = getTransactionReplicas(transId);
		
		val gr = GlobalRef[MapRequest](request);
		
		asyncWaitForResponse(request, null);
		
	    var exception:Exception = null;
		for (placeId in targetReplicas) {
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
			System.threadSleep(10);
			try{
				lock.lock();
				var i:Long;
				if (VERBOSE) Utils.console(moduleName, "Check pending transactions new iteration ...");
				for (i = 0; i< pendingRequests.size(); i++){
					val curReq = pendingRequests.get(i);
					
					val mapReq = curReq.req;
					if (mapReq.completed) {
						mapReq.lock.release();
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
						//TODO: because commit require multiple interaction- we need to save the last interaction time
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
		}
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