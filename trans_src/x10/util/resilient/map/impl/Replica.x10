package x10.util.resilient.map.impl;

import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicInteger;
import x10.util.ArrayList;
import x10.util.RailUtils;
import x10.util.HashMap;
import x10.util.HashSet;
import x10.util.Timer;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.transaction.TransLog;
import x10.util.resilient.map.exception.CommitedTransactionCanNotBeAbortedException;
import x10.util.resilient.map.exception.TransactionNotFoundException;
import x10.util.resilient.map.partition.Partition;
import x10.util.resilient.map.partition.VersionValue;

//Concurrency:  multiple threads
public class Replica {
	private val moduleName = "Replica("+here.id+")";
	public static val VERBOSE = Utils.getEnvLong("REPLICA_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;

	/**Transaction Status**/
	public static val TRANS_ACTIVE:Int = 0n;
	public static val TRANS_READY_TO_COMMIT:Int = 1n;
	public static val TRANS_COMMITED:Int = 2n;
	public static val TRANS_ABORTED:Int = 3n;
	
	// Dummy transaction log used to replace aborted transactions
	private static val DUMMY_TRANSACTION = new TransLog(-1, -1, -1); 
	
	//transaction_status::transactions
	private val transactions:HashMap[Int,TransactionsMap];
	private val transactionsLock:SimpleLatch;
	
    private val partitions:HashMap[Long,Partition];   
	private val partitionsLock:SimpleLatch;

	private var timerOn:Boolean = false;
	
    public def this(partitionIds:HashSet[Long]) {
    	partitions = new HashMap[Long,Partition]();
    	createPartitions(partitionIds);
    	partitionsLock = new SimpleLatch();
    	transactionsLock = new SimpleLatch();
    	transactions = new HashMap[Int,TransactionsMap]();
    	transactions.put(TRANS_ACTIVE, new TransactionsMap());
    	transactions.put(TRANS_READY_TO_COMMIT, new TransactionsMap());
    	transactions.put(TRANS_COMMITED, new TransactionsMap());
    	transactions.put(TRANS_ABORTED, new TransactionsMap());
    }
    
    private def createPartitions(partitionIds:HashSet[Long]) {
    	val iter = partitionIds.iterator();
    	while (iter.hasNext()) {
    		val id = iter.next();
    		partitions.put(id,new Partition(id));
    	}
    }
    
    public def submitSingleKeyRequest(mapName:String, clientId:Long, paritionId:Long, transId:Long, requestType:Int, key:Any, value:Any, responseGR:GlobalRef[MapRequest]) {
    	switch(requestType) {
			case MapRequest.REQ_GET: get(mapName, clientId, paritionId, transId, key, value, responseGR); break;
			case MapRequest.REQ_PUT: put(mapName, clientId, paritionId, transId, key, value, responseGR); break;
			case MapRequest.REQ_DELETE: delete(mapName, clientId, paritionId, transId, key, responseGR); break;
    	}
    }
    
    public def get(mapName:String, clientId:Long, paritionId:Long, transId:Long, key:Any, value:Any, responseGR:GlobalRef[MapRequest]) {
    	val transLog = getOrAddActiveTransaction(transId, clientId);
    	if (transLog == null) {
    		at (responseGR.home) async {
        		responseGR().addReplicaResponse(returnValue, new TransactionAbortedException(), replicaId);
        	}
    		return;
    	}

    	val partition = partitions.getOrThrow(paritionId);
    	var verValue:VersionValue = partition.getV(mapName, key);
    	var oldValue:Any = null;
    	if (verValue == null)
    		transLog.logGet (key, -1n, null, paritionId);
    	else {
    		transLog.logGet (key, verValue.getVersion(), verValue.getValue(), paritionId);
    		oldValue = verValue.getValue();
    	}
    	
    	val replicaId = here.id;
    	val returnValue = oldValue;
    	at (responseGR.home) async {
    		responseGR().addReplicaResponse(returnValue, null, replicaId);
    	}
    }
    
    public def put(mapName:String, clientId:Long, paritionId:Long, transId:Long, key:Any, value:Any, responseGR:GlobalRef[MapRequest]) {    	
    	val transLog = getOrAddActiveTransaction(transId, clientId);
    	if (transLog == null) {
    		at (responseGR.home) async {
        		responseGR().addReplicaResponse(returnValue, new TransactionAbortedException(), replicaId);
        	}
    		return;
    	}

    	val partition = partitions.getOrThrow(paritionId);
    	var verValue:VersionValue = partition.getV(mapName, key);
    	var oldValue:Any = null;
    	if (verValue == null)
    		transLog.logUpdate(key, -1n, null, value, paritionId);
    	else {
    		transLog.logUpdate(key, verValue.getVersion(), verValue.getValue(), value, paritionId);
    		oldValue = verValue.getValue();
    	}
   	
    	val replicaId = here.id;
    	val returnValue = oldValue;
    	at (responseGR) async {
    		responseGR().addReplicaResponse(returnValue, null, replicaId);
    	}
    }
    
    public def delete(mapName:String, clientId:Long, paritionId:Long, transId:Long, key:Any, responseGR:GlobalRef[MapRequest]) {
       	val transLog = getOrAddActiveTransaction(transId, clientId);
    	if (transLog == null) {
    		at (responseGR.home) async {
        		responseGR().addReplicaResponse(returnValue, new TransactionAbortedException(), replicaId);
        	}
    		return;
    	}

    	val partition = partitions.getOrThrow(paritionId);
    	var verValue:VersionValue = partition.getV(mapName, key);
    	var oldValue:Any = null;
    	if (verValue == null)
    		transLog.logDelete(key, -1n, null, paritionId);
    	else {
    		transLog.logDelete(key, verValue.getVersion(), verValue.getValue(), paritionId);
    		oldValue = verValue.getValue();
    	}
   	
    	val replicaId = here.id;
    	val returnValue = oldValue;
    	at (responseGR) async {
    		responseGR().addReplicaResponse(returnValue, null, replicaId);
    	}
    }
    
    //TODO: can we allow commit for multiple Maps???
    public def submitReadyToCommit(transId:Long, responseGR:GlobalRef[MapRequest]) {    	
    	val ready = readyToCommit(transId);
    	val vote = ready? 1 : 0;
    	val replicaId = here.id;
    	at (responseGR) async {
    		responseGR().commitVote(vote, replicaId);
    	}
    }	
    
    //no exception is expected from this method
    public def submitConfirmCommit(transId:Long, responseGR:GlobalRef[MapRequest]) {    
    	val transLog = getTransactionLog(TRANS_READY_TO_COMMIT, transId);
    	
    	val replicaId = here.id;
    	at (responseGR) async {
    		responseGR().addReplicaResponse(null, null, replicaId);
    	}
    }
    
    public def submitAbort(transId:Long, responseGR:GlobalRef[MapRequest]) {
    	try{
			transactionsLock.lock();
			var transLog:TransLog = transactions.getOrThrow(TRANS_ACTIVE).transMap.remove(transId);
			if (transLog == null) {
				transLog = transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap.remove(transId);
				if (transLog == null) {
					transLog = transactions.getOrThrow(TRANS_ABORTED).transMap.remove(transId);
					if (transLog == null) {
						transLog = transactions.getOrThrow(TRANS_COMMITED).transMap.remove(transId);
						if (transLog != null)
							throw new CommitedTransactionCanNotBeAbortedException();
						else
							throw new TransactionNotFoundException();
					}
				}
			}
			
			if (transLog != null)
				transactions.getOrThrow(TRANS_ABORTED).transMap.put(transId, DUMMY_TRANSACTION);
    	}
		finally {
			transactionsLock.unlock();
		}
    }
    
    
    private def getOrAddActiveTransaction(transId:Long, clientId:Long):TransLog {
    	var result:TransLog = null;
		try{
			transactionsLock.lock();
			result = transactions.getOrThrow(TRANS_ACTIVE).transMap.getOrElse(transId,null);
			if (result == null) {
				val x = transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap.getOrElse(transId,null);
				val y = transactions.getOrThrow(TRANS_COMMITED).transMap.getOrElse(transId,null);
				val z = transactions.getOrThrow(TRANS_ABORTED).transMap.getOrElse(transId,null);
				
				if (x == null && y == null && z == null) {
					result = new TransLog(transId,Timer.milliTime(), clientId);
					transactions.getOrThrow(TRANS_ACTIVE).transMap.put(transId,result);
				}
			}
		}
		finally {
			transactionsLock.unlock();
		}
		return result;
    }
    
    private def getTransactionLog(status:Int, transId:Long):TransLog {
    	var result:TransLog = null;
		try{
			transactionsLock.lock();
			result = transactions.getOrThrow(status).transMap.getOrElse(transId,null);
		}
		finally {
			transactionsLock.unlock();
		}
		return result;
    }
    
    public def addMap(mapName:String) {
    	try{
    		partitionsLock.lock();
    		val iter = partitions.keySet().iterator();
    		while (iter.hasNext()) {
    			val partId = iter.next();
    			val part = partitions.getOrThrow(partId);
    			part.addMap(mapName);
    		}	
    	}
    	finally{
    		partitionsLock.unlock();
    	}
    }
    
    //assumes the current transaction is Active
    private def readyToCommit(transId:Long):Boolean {
    	var ready:Boolean = true;
    	try{
    		transactionsLock.lock();
    		
    		if (!conflictingWithReadyTransaction(transId)
    			&& sameInitialVersions(transId)) {
    			val conflictReport = getConflictingActiveTransactions(transId);
    			if (conflictReport != null) {
    				if (conflictReport.maxTransId == transId) {
    				//	abort others
    					for (otherTransId in conflictReport.otherTransactions) {
    						val curTrans = transactions.getOrThrow(TRANS_ACTIVE).transMap.remove(otherTransId.transId);
    						transactions.getOrThrow(TRANS_ABORTED).transMap.put(otherTransId.transId, DUMMY_TRANSACTION);
    					}
    				}
    				else {
    					// abort my self
    					ready = false;
    					val curTrans = transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap.remove(transId);
						transactions.getOrThrow(TRANS_ABORTED).transMap.put(transId, DUMMY_TRANSACTION);
    				}
    			}
    		} 
    		
    		if (ready && !timerOn)
    			async checkDeadClientForReadyTransactions();
    	}finally {
    		transactionsLock.unlock();
    	}
    	return ready;
    }
    
    private def conflictingWithReadyTransaction(transId:Long):Boolean {
    	return false;
    }
    
    private def sameInitialVersions(transId:Long):Boolean {
    	return true;
    }
    
    private def getConflictingActiveTransactions(transId:Long):ConflictReport {
    	return null;
    }
    
    //TODO: should also check active transactions
    public def checkDeadClientForReadyTransactions () {
    	while (timerOn) {
    		System.threadSleep(10);
    		try{
    			transactionsLock.lock();
    			val readyTransMap = transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap;
    			val iter = readyTransMap.keySet().iterator();
    			while (iter.hasNext()) {
    				val transId = iter.next();
    				val curTrans = readyTransMap.getOrThrow(transId);
    				if (Place(curTrans.clientPlaceId).isDead()) {
    					readyTransMap.remove(transId);
    					transactions.getOrThrow(TRANS_ABORTED).transMap.put(transId, DUMMY_TRANSACTION);
    				}
    			}
    			
    			if (readyTransMap.size() == 0){
    				timerOn = false;
    			}
    		}
    		finally{
    			transactionsLock.unlock();
    		}
    	}
    }
    
    public def toString():String {
    	var str:String = "Primary [";
    	
        str += "]\n";
        return str;
    }
}

class TransactionsMap {
	public val transMap:HashMap[Long,TransLog] = new HashMap[Long,TransLog]();
}

class ConflictReport {
	public val otherTransactions:ArrayList[TransLog] = new ArrayList[TransLog]();
    public var maxTransId:Long = -1000;
}