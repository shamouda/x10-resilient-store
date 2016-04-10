import x10.util.concurrent.SimpleLatch;
import x10.util.ArrayList;
import x10.util.RailUtils;
import x10.util.HashMap;
import x10.util.HashSet;
import x10.util.Timer;

//Concurrency:  multiple threads
public class Replica {
	private val moduleName = "Replica("+here.id+")";
	public static val VERBOSE = Utils.getEnvLong("REPLICA_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
	
    private val paritions:HashMap[Long,Partition] = new HashMap[Long,Partition]();    
	private val partitionsLock:SimpleLatch;
	
	
	private val transactions:HashMap[Long,TransLog] = new HashMap[Long,TransLog]();
	private val transactionsLock:SimpleLatch;

    public def this(partitionIds:HashSet[Long]) {	       
    	createPartitions(partitionIds);
    	partitionsLock = new SimpleLatch();
    	transactionsLock = new SimpleLatch();
    }
    
    private def createPartitions(partitionIds:HashSet[Long]) {
    	val iter = partitionIds.iterator();
    	while (iter.hasNext()) {
    		val key = iter.next();
    		paritions.put(key,new Partition(key));
    	}
    }
    
    public def submitSingleKeyRequest(mapName:String, clientId:Long, paritionId:Long, transId:Long, requestType:Int, key:Any, value:Any, timeoutMillis:Long, responseGR:GlobalRef[MapRequest]) {
    	switch(requestType) {
			case MapRequest.REQ_GET: get(mapName, clientId, paritionId, transId, key, value, timeoutMillis, responseGR);
			case MapRequest.REQ_PUT: put(mapName, clientId, paritionId, transId, key, value, timeoutMillis, responseGR);
			case MapRequest.REQ_DELETE: delete(mapName, clientId, paritionId, transId, key, value, timeoutMillis, responseGR);
    	}
    }
    
    public def get(mapName:String, clientId:Long, paritionId:Long, transId:Long, key:Any, value:Any, timeoutMillis:Long, responseGR:GlobalRef[MapRequest]) {
    	Utils.console(moduleName, "Received Get Request ...");    	
    	val transLog = getTransactionLog(transId);

    	val parition = paritions.getOrThrow(paritionId);
    	val oldValue = parition.get(mapName, key);
    	transLog.addLog(key, oldValue);
    	val replicaId = here.id;
    	at (responseGR.home) async {
    		responseGR().addReplicaResponse(oldValue, replicaId);
    	}
    }
    
    public def put(mapName:String, clientId:Long, paritionId:Long, transId:Long, key:Any, value:Any, timeoutMillis:Long, responseGR:GlobalRef[MapRequest]) {    	
    	val transLog = getTransactionLog(transId);

    	val parition = paritions.getOrThrow(paritionId);
    	val oldValue = parition.get(mapName, key);
    	transLog.addLog(key, oldValue);
    	transLog.updateLog(key, value);
    	
    	val replicaId = here.id;
    	at (responseGR) async {
    		responseGR().addReplicaResponse(oldValue, replicaId);
    	}
    }
    
    public def delete(mapName:String, clientId:Long, paritionId:Long, transId:Long, key:Any, value:Any, timeoutMillis:Long, responseGR:GlobalRef[MapRequest]) {
    
    }
        
    	
    
    private def getTransactionLog(transId:Long):TransLog {
    	var result:TransLog = null;
    	try{
    		//TODO: do I need to lock here????
    		transactionsLock.lock();
    		result = transactions.getOrElse(transId,null);
    		if (result == null) {
    			result = new TransLog(transId,Timer.milliTime());
    			transactions.put(transId,result);
    		}
    	}
    	finally {
    		transactionsLock.unlock();
    	}
    	return result;
    }

    public def addMap(mapName:String) {
    	try{
    		partitionsLock.lock();
    		val iter = paritions.keySet().iterator();
    		while (iter.hasNext()) {
    			val partId = iter.next();
    			val part = paritions.getOrThrow(partId);
    			part.addMap(mapName);
    		}	
    	}
    	finally{
    		partitionsLock.unlock();
    	}
    	
    }
    
    public def checkPendingTransactions () {
    	
    }
    
    public def toString():String {
    	var str:String = "Primary [";
    	
        str += "]\n";
        return str;
    }
}