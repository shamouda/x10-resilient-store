import x10.util.ArrayList;
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

	public def asyncExecuteRequest(mapName:String, request:MapRequest, timeoutMillis:Long) {
		if (VERBOSE) Utils.console(moduleName, "Submitting request: " + request.toString());
		addRequest(request, timeoutMillis);
		
		switch(request.requestType) {
			case MapRequest.REQ_GET: asyncExecuteSingleKeyRequest(mapName, request, timeoutMillis);
			case MapRequest.REQ_PUT: asyncExecuteSingleKeyRequest(mapName, request, timeoutMillis);
			case MapRequest.REQ_DELETE: asyncExecuteSingleKeyRequest(mapName, request, timeoutMillis);
			case MapRequest.REQ_KEY_SET: asyncExecuteKeySet(mapName, request, timeoutMillis);
			case MapRequest.REQ_COMMIT: asyncExecuteCommit(mapName, request, timeoutMillis);
			case MapRequest.REQ_ABORT: asyncExecuteAbort(mapName, request, timeoutMillis);
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
	
	private def asyncExecuteSingleKeyRequest(mapName:String, request:MapRequest, timeoutMillis:Long) {
		val key = request.inKey;
		val value = request.inValue;
		val requestType = request.requestType;
		val transId = request.transactionId;
		
		if (VERBOSE) Utils.console(moduleName, "Reading the replicas ...");
		val repInfo = partitionTable.getKeyReplicas(key);
		if (VERBOSE) Utils.console(moduleName, repInfo.toString());
		request.setResponseReplicas(repInfo.replicas);
		val gr = GlobalRef[MapRequest](request);
		for (placeId in repInfo.replicas) {
			try{
				at (Place(placeId)) async {
					DataStore.getInstance().getReplica().submitSingleKeyRequest(mapName, here.id, repInfo.partitionId, transId, requestType, key, value, timeoutMillis, gr);
				}
			}
			catch (ex:Exception) {
				request.completeFailedRequest(ex);
				break;
			}
		}
	}
	

	
	private def asyncExecuteKeySet(mapName:String, request:MapRequest, timeoutMillis:Long) {
		
	}
	

	private def asyncExecuteCommit(mapName:String, request:MapRequest, timeoutMillis:Long) {
		
	}
	
	private def asyncExecuteAbort(mapName:String, request:MapRequest, timeoutMillis:Long) {
		
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