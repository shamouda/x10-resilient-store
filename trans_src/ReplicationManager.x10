import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;

public class ReplicationManager {
	private val moduleName = "ReplicationManager";
	public static val VERBOSE = Utils.getEnvLong("REPL_MNGR_VERBOSE", 0) == 1;
	
    //this is the class that communicates with the replicas
    private var partitionTable:PartitionTable;
    private var migrationStarted:Boolean;
	
	private val pendingRequests:ArrayList[MapRequest];
	private val lock:SimpleLatch;
    
    public def this(partitionTable:PartitionTable) {
    	this.partitionTable = partitionTable;
    	pendingRequests = new ArrayList[MapRequest]();
    	lock = new SimpleLatch(); 
    }

	public def asyncExecuteRequest(mapName:String, request:MapRequest, timeoutMillis:Long) {
		if (VERBOSE) Utils.console(moduleName, "Submitting request: " + request.toString());
		request.success = true;
		addRequest(request);
		async checkPendingTransactions();
	}
	
	public def updatePartitionTable(PartitionTable) {
		
	}
	
	public def setMigrationStatus(migrating:Boolean) {
		migrationStarted = migrating;
	}
	
	private def addRequest(req:MapRequest) {
		try{
			lock.lock();
			pendingRequests.add(req);
		}
		finally{
			lock.unlock();
		}
	}
	
	private def checkPendingTransactions() {
		
		while (true) 
		{
			System.threadSleep(10);
			try{
				lock.lock();
				for (p in pendingRequests){
					p.lock.release();
				}
		
			}
			finally{
				lock.unlock();
			}
		}
	}
}