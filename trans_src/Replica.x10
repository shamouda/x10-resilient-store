import x10.util.concurrent.SimpleLatch;
import x10.util.ArrayList;
import x10.util.RailUtils;
import x10.util.HashMap;
import x10.util.HashSet;
import x10.util.Timer;

//Concurrency:  multiple threads
public class Replica {
	private val replicaId = here.id;
	
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
    
    public def submitSingleKeyRequest(mapName:String, paritionId:Long, request:MapRequest, timeoutMillis:Long, responseGR:GlobalRef[MapRequest]) {
    	switch(request.requestType) {
			case MapRequest.REQ_GET: get(mapName, paritionId, request, timeoutMillis, responseGR);
			case MapRequest.REQ_PUT: put(mapName, paritionId, request, timeoutMillis, responseGR);
			case MapRequest.REQ_DELETE: delete(mapName, paritionId, request, timeoutMillis, responseGR);
    	}
    }
    
    public def get(mapName:String, paritionId:Long, request:MapRequest, timeoutMillis:Long, responseGR:GlobalRef[MapRequest]) {
    	val key = request.inKey;
    	val transId = request.transactionId;
    	val transLog = getTransactionLog(transId);

    	val parition = paritions.getOrThrow(paritionId);
    	val value = parition.get(mapName, key);
    	transLog.addLog(key, value);
    	
    	at (responseGR.home) async {
    		responseGR().addReplicaResponse(value, here.id);
    	}
    }
    
    public def put(mapName:String, paritionId:Long, request:MapRequest, timeoutMillis:Long, responseGR:GlobalRef[MapRequest]) {
    	val key = request.inKey;
    	val value = request.inValue;
    	val transId = request.transactionId;
    	val transLog = getTransactionLog(transId);

    	val parition = paritions.getOrThrow(paritionId);
    	val oldValue = parition.get(mapName, key);
    	transLog.addLog(key, oldValue);
    	transLog.updateLog(key, value);
    	
    	at (responseGR.home) async {
    		responseGR().addReplicaResponse(oldValue. here.id);
    	}
    }
    
    private def getTransactionLog(transId:Long):TransLog {
    	var result:TransLog = null;
    	try{
    		//TODO: do I need to lock here????
    		transactionsLock.lock();
    		result = transactions.getOrElse(transId,null);
    		if (result == null) {
    			result = new TransLog(transId,Timer.milliTime());
    			transactions.put(result);
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