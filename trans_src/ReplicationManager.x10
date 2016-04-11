import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;

public class ReplicationManager {
	private val moduleName = "ReplicationManager";
	public static val VERBOSE = Utils.getEnvLong("REPL_MNGR_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
    //this is the class that communicates with the replicas
    private var partitionTable:PartitionTable;
    private var migrationStarted:Boolean;
	
	private val pendingRequests:ArrayList[RequestState];
	private val lock:SimpleLatch;
	
	private var timerOn:Boolean = false;
    
    public def this(partitionTable:PartitionTable) {
    	this.partitionTable = partitionTable;
    	pendingRequests = new ArrayList[RequestState]();
    	lock = new SimpleLatch(); 
    }

	public def asyncExecuteRequest(request:MapRequest, timeoutMillis:Long) {
		if (VERBOSE) Utils.console(moduleName, "Submitting request: " + request.toString());
		addRequest(request, timeoutMillis);
		
		switch(request.requestType) {
			case MapRequest.REQ_GET: asyncExecuteSingleKeyRequest(request);
			case MapRequest.REQ_PUT: asyncExecuteSingleKeyRequest(request);
			case MapRequest.REQ_DELETE: asyncExecuteSingleKeyRequest(request);
			case MapRequest.REQ_KEY_SET: asyncExecuteKeySet(request);
			case MapRequest.REQ_COMMIT: asyncExecuteCommit(request);
			case MapRequest.REQ_ABORT: asyncExecuteAbort(request);
		}		
	}
	
	private def addRequest(req:MapRequest, timeoutMillis:Long) {
		try{
			lock.lock();
			pendingRequests.add(new RequestState(req, timeoutMillis, Timer.milliTime()));
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
	
	private def asyncExecuteSingleKeyRequest(request:MapRequest) {
		val key = request.inKey;
		val value = request.inValue;
		val mapName = request.mapName;
		val requestType = request.requestType;
		val transId = request.transactionId;
		
		val repInfo = partitionTable.getKeyReplicas(key);
		request.setResponseReplicas(repInfo.replicas);

		val gr = GlobalRef[MapRequest](request);
		
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

	private def asyncExecuteKeySet(request:MapRequest) {
		
	}
	

	private def asyncExecuteCommit(request:MapRequest) {
		val requestType = request.requestType;
		val transId = request.transactionId;
		val mapName = request.mapName;
		
		val replicas = getTransactionReplicas(transId);
		request.setResponseReplicas(replicas);

		val gr = GlobalRef[MapRequest](request);
		
		for (placeId in replicas) {
			try{
				at (Place(placeId)) async {
					DataStore.getInstance().getReplica().submitReadyToCommit(transId, gr);
				}
			}
			catch (ex:Exception) {
				//TODO: what to do?
				//request.completeFailedRequest(ex);
				//break;
			}
		}	
	}
	

	private def asyncExecuteConfirmCommit(request:MapRequest) {
		val requestType = request.requestType;
		val transId = request.transactionId;
		val mapName = request.mapName;
		val replicas = getTransactionReplicas(transId);
		request.setResponseReplicas(replicas);

		val gr = GlobalRef[MapRequest](request);
		
		for (placeId in replicas) {
			try{
				at (Place(placeId)) async {
                    DataStore.getInstance().getReplica().submitConfirmCommit(transId, gr);
				}
			}
			catch (ex:Exception) {
				//TODO: what to do here?
				//request.completeFailedRequest(ex);
				//break;
			}
		}	
	}
	
	private def asyncExecuteAbort(request:MapRequest) {
		val transId = request.transactionId;
		
		var targetReplicas:HashSet[Long] = request.replicasVotedToCommit;
	    if (targetReplicas == null)
	    	targetReplicas = request.replicas;

	    val gr = GlobalRef[MapRequest](request);
	    
		for (placeId in targetReplicas) {
			try{
				at (Place(placeId)) async {
					DataStore.getInstance().getReplica().submitAbort(transId, gr);
				}
			}
			catch (ex:Exception) {
				//TODO: what to do here
				//request.completeFailedRequest(ex);
				//break;
			}
		}
	}

	public def getTransactionReplicas(transactionId:Long):HashSet[Long] {
		val result = new HashSet[Long]();
		try{
			lock.lock();
			
			for (var i:Long = 0; i < pendingRequests.size(); i++){
				val curReq = pendingRequests.get(i);
				if (curReq.req.transactionId == transactionId)
					result.addAll(curReq.req.replicas);
			}
		}
		finally{
			lock.unlock();
		}
		return result;
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
	
	public def setMigrationStatus(migrating:Boolean) {
		migrationStarted = migrating;
	}
	
	private def checkPendingTransactions() {
		while (timerOn) {
			System.threadSleep(10);
			try{
				lock.lock();
				var i:Long;
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
							asyncExecuteConfirmCommit(mapReq);
						}
						else if (mapReq.commitStatus == MapRequest.CANCELL_COMMIT) {
							asyncExecuteAbort(mapReq);
						}
					}
					else if (Timer.milliTime() - curReq.startTimeMillis > curReq.timeoutMillis) {	
						mapReq.completeFailedRequest(new RequestTimeoutException());
						mapReq.lock.release();
						pendingRequests.removeAt(i);
						i--;
					} else {
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
    public val timeoutMillis:Long;
    public val startTimeMillis:Long;
    public var timeOut:Boolean;
    public def this (req:MapRequest, timeoutMillis:Long, startTimeMillis:Long) {
        this.req = req;
        this.timeoutMillis = timeoutMillis;
        this.startTimeMillis = startTimeMillis;
    }
	
}