import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;
import x10.util.Timer;

public class ReplicationManager {
	private val moduleName = "ReplicationManager";
	public static val VERBOSE = Utils.getEnvLong("REPL_MNGR_VERBOSE", 0) == 1;
	
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
	}
	
	public def updatePartitionTable(p:PartitionTable) {
		
	}
	
	public def setMigrationStatus(migrating:Boolean) {
		migrationStarted = migrating;
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
	}
	
	private def checkPendingTransactions() {
		
		while (timerOn) 
		{
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
						mapReq.completed = true;
						mapReq.outException = new RequestTimeoutException();
						mapReq.lock.release();
						pendingRequests.removeAt(i);
						i--;
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