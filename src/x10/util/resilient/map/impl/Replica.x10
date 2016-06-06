package x10.util.resilient.map.impl;

import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicInteger;
import x10.util.ArrayList;
import x10.util.RailUtils;
import x10.util.HashMap;
import x10.util.HashSet;
import x10.util.Timer;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.transaction.Transaction;
import x10.util.resilient.map.exception.CommitedTransactionCanNotBeAbortedException;
import x10.util.resilient.map.exception.TransactionNotFoundException;
import x10.util.resilient.map.exception.TransactionAbortedException;
import x10.util.resilient.map.partition.Partition;
import x10.util.resilient.map.partition.VersionValue;
import x10.util.resilient.map.migration.MigrationRequest;
import x10.util.resilient.map.DataStore;

/*
 * Replica is a container for partitions and logs of transactions accessing them
 * 
 * */
public class Replica {
    private val moduleName = "Replica("+here.id+")";
    public static val VERBOSE = Utils.getEnvLong("REPLICA_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;

    /**Transaction Status**/
    public static val TRANS_ACTIVE:Int = 0n;
    public static val TRANS_WAITING:Int = 1n;
    public static val TRANS_PRE_COMMIT:Int = 2n;
    public static val TRANS_COMMITED:Int = 3n;
    public static val TRANS_ABORTED:Int = 4n;
    public static val TRANS_BLOCKED:Int = -5n; // Used in 2PC to mark transactions that lost their coordinator
    
    public static val READY_YES:Long = 1;
    public static val READY_NO:Long = 0;
    
    // Dummy transaction log used to replace aborted/commited transactions
    private static val DUMMY_TRANSACTION = new Transaction(-1, -1, -1); 
    
    //transaction_status::transactions
    private val transactions:HashMap[Int,TransactionsMap];
    private val migratingPartitions:HashSet[Long];
    private val transactionsLock:SimpleLatch;
    
    private val partitions:HashMap[Long,Partition];
    private val partitionsLock:SimpleLatch;
    
    private val notifiedTransactions = new ArrayList[Long]();
    
    private var timerOn:Boolean = false;
    
    public def this(partitionIds:HashSet[Long]) {
        partitions = new HashMap[Long,Partition]();
        migratingPartitions = new HashSet[Long]();
        createPartitions(partitionIds);
        transactionsLock = new SimpleLatch();
        partitionsLock = new SimpleLatch();
        transactions = new HashMap[Int,TransactionsMap]();
        transactions.put(TRANS_ACTIVE, new TransactionsMap());
        transactions.put(TRANS_WAITING, new TransactionsMap());
        transactions.put(TRANS_PRE_COMMIT, new TransactionsMap());
        transactions.put(TRANS_COMMITED, new TransactionsMap());
        transactions.put(TRANS_ABORTED, new TransactionsMap());
        transactions.put(TRANS_BLOCKED, new TransactionsMap());
    }
    
    private def createPartitions(partitionIds:HashSet[Long]) {
        val iter = partitionIds.iterator();
        while (iter.hasNext()) {
            val id = iter.next();
            partitions.put(id,new Partition(id));
        }
    }
    
    private def getVersionValue(partitionId:Long, mapName:String, key:Any):VersionValue {
        var verValue:VersionValue = null;
        try{
            partitionsLock.lock();
            val partition = partitions.getOrThrow(partitionId);
            verValue = partition.getV(mapName, key);
        } finally{
            partitionsLock.unlock();
        }
        return verValue;
    }
    
    
    public def submitSingleKeyRequest(mapName:String, clientId:Long, partitionId:Long, transId:Long, requestType:Int, key:Any, value:Any, replicas:HashSet[Long], responseGR:GlobalRef[MapRequest]) {
         switch(requestType) {
            case MapRequest.REQ_GET: get(mapName, clientId, partitionId, transId, key, value, replicas, responseGR); break;
            case MapRequest.REQ_PUT: put(mapName, clientId, partitionId, transId, key, value, replicas, responseGR); break;
            case MapRequest.REQ_DELETE: delete(mapName, clientId, partitionId, transId, key, replicas, responseGR); break;
        }
    }
    
    public def get(mapName:String, clientId:Long, partitionId:Long, transId:Long, key:Any, value:Any, replicas:HashSet[Long], responseGR:GlobalRef[MapRequest]) {
        val transLog = getOrAddActiveTransaction(transId, clientId);
        val replicaId = here.id;
        if (transLog == null) {
            if (VERBOSE) Utils.console(moduleName, "(get) Transaction ["+transId+"]  is not active. Return TransactionAbortedException ...");
            at (responseGR.home) async {
                responseGR().addReplicaResponse(null, new TransactionAbortedException(), replicaId);
            }
            return;
        }
        
        var oldValue:Any = null;
        val cache = transLog.getKeysCache();
        val keyLog = cache.getOrElse(key,null);
        if (keyLog != null) { // key used before in the transaction
            oldValue = keyLog.getValue();
        }
        else {
            val verValue = getVersionValue(partitionId, mapName, key);
            if (verValue == null)
                transLog.logGet (key, -1n, null, partitionId);
            else {
                transLog.logGet (key, verValue.getVersion(), verValue.getValue(), partitionId);
                oldValue = verValue.getValue();
            }
        }
        
        val returnValue = oldValue;
        at (responseGR.home) async {
            responseGR().addReplicaResponse(returnValue, null, replicaId);
        }
    }
    
    public def put(mapName:String, clientId:Long, partitionId:Long, transId:Long, key:Any, value:Any, replicas:HashSet[Long], responseGR:GlobalRef[MapRequest]) {        
        val transLog = getOrAddActiveTransaction(transId, clientId);
        val replicaId = here.id;
        if (transLog == null) {
            if (VERBOSE) Utils.console(moduleName, "(put) Transaction ["+transId+"]  is not active. Return TransactionAbortedException ...");
            at (responseGR.home) async {
                responseGR().addReplicaResponse(null, new TransactionAbortedException(), replicaId);
            }
            return;
        }

        var oldValue:Any = null;
        
        val cache = transLog.getKeysCache();
        val keyLog = cache.getOrElse(key,null);
        if (keyLog != null) { // key used in the transaction before
            oldValue = keyLog.getValue();
            transLog.logUpdate(key, value);
        }
        else {// first time to access this key
            val verValue = getVersionValue(partitionId, mapName, key);
            
            if (verValue == null)
                transLog.logUpdate(key, -1n, null, value, partitionId);
            else {
                oldValue = verValue.getValue();
                transLog.logUpdate(key, verValue.getVersion(), verValue.getValue(), value, partitionId);
            }
        }
        val returnValue = oldValue;
        at (responseGR) async {
            responseGR().addReplicaResponse(returnValue, null, replicaId);
        }
    }
    
    public def delete(mapName:String, clientId:Long, partitionId:Long, transId:Long, key:Any, replicas:HashSet[Long], responseGR:GlobalRef[MapRequest]) {
        val transLog = getOrAddActiveTransaction(transId, clientId);
        val replicaId = here.id;
        if (transLog == null) {
            if (VERBOSE) Utils.console(moduleName, "(delete) Transaction ["+transId+"]  is not active. Return TransactionAbortedException ...");
            at (responseGR.home) async {
                responseGR().addReplicaResponse(null, new TransactionAbortedException(), replicaId);
            }
            return;
        }
        var oldValue:Any = null;
        val cache = transLog.getKeysCache();
        val keyLog = cache.getOrElse(key,null);
        if (keyLog != null) { // key used in the transaction before
            oldValue = keyLog.getValue();
            transLog.logDelete(key);
        }
        else {// first time to access this key
            val verValue = getVersionValue(partitionId, mapName, key);
            
            if (verValue == null)
                transLog.logDelete(key, -1n, null, partitionId);
            else {
                oldValue = verValue.getValue();
                transLog.logDelete(key, verValue.getVersion(), verValue.getValue(), partitionId);
            }
        }
        val returnValue = oldValue;
        at (responseGR) async {
            responseGR().addReplicaResponse(returnValue, null, replicaId);
        }
    }
    
    public def prepareCommit(mapName:String, transId:Long, responseGR:GlobalRef[MapRequest]) { 
        val replicaId = here.id;
        val transLog = getTransactionLog(TRANS_ACTIVE, transId);
        if (transLog == null) {
            if (VERBOSE) Utils.console(moduleName, "(ready) Transaction ["+transId+"]  is not active. Return Not Ready ...");
            at (responseGR) async {
                responseGR().commitVote(READY_NO, replicaId);
            }
            return;
        }
        
        val ready = readyToCommit(mapName, transLog);
        val vote = ready? READY_YES : READY_NO;
        
        at (responseGR) async {
            responseGR().commitVote(vote, replicaId);
        }
    }    
    
    
    public def commitNoResponse(mapName:String, transId:Long, responseGR:GlobalRef[MapRequest]) {   
        val transLog = getTransactionLog(TRANS_WAITING, transId);
        val cache = transLog.getKeysCache();
        val keysIter = cache.keySet().iterator();

        try{
            partitionsLock.lock();
            
            while (keysIter.hasNext()) {
            	val key = keysIter.next();
            	val log = cache.getOrThrow(key);
            	if (log.readOnly()) continue;
                val partition = partitions.getOrThrow(log.getPartitionId());
               	if (VERBOSE) Utils.console(moduleName, "TransId["+transId+"] Applying commit changes:=> " + log.toString());
               	if (log.isDeleted()) {
               		partition.delete(mapName, key);
               	}
               	else if (!log.readOnly()) {
               		partition.put(mapName, key, log.getValue());
               	}
            }
        }
        finally{
            partitionsLock.unlock();
        }
        
        try{
            transactionsLock.lock();
            transactions.getOrThrow(TRANS_WAITING).transMap.remove(transId);
            transactions.getOrThrow(TRANS_COMMITED).transMap.put(transId, DUMMY_TRANSACTION);
        }finally {
            transactionsLock.unlock();
        }
    }
    
    public def abortNoResponse(transId:Long, responseGR:GlobalRef[MapRequest]) {
        if (VERBOSE) Utils.console(moduleName, "calling abortNoResponse for transactionId["+transId+"] ...");
        try{
            transactionsLock.lock();
            var transLog:Transaction = transactions.getOrThrow(TRANS_ACTIVE).transMap.remove(transId);
            if (transLog == null) {
                transLog = transactions.getOrThrow(TRANS_WAITING).transMap.remove(transId);
                if (transLog == null) {
                    transLog = transactions.getOrThrow(TRANS_ABORTED).transMap.remove(transId);
                    if (transLog == null) {
                        transLog = transactions.getOrThrow(TRANS_COMMITED).transMap.remove(transId);
                        if (transLog != null)
                            throw new CommitedTransactionCanNotBeAbortedException();
                        else { // transaction not found, this can be due to processing an abort message before an add message                            
                            if (VERBOSE) Utils.console(moduleName, "FATAL error: abortNoResponse transactionId["+transId+"] not found!!!  Transaction aborted before being added  ...");
                        }
                    }
                }
            }
            transactions.getOrThrow(TRANS_ABORTED).transMap.put(transId, DUMMY_TRANSACTION);
        }
        finally {
            transactionsLock.unlock();
        }
    }
    
    private def getOrAddActiveTransaction(transId:Long, clientId:Long, replicas:HashSet[Long]):Transaction {
    	if (VERBOSE) Utils.console(moduleName, "getOrAddActiveTransaction transId["+transId+"] clientId["+clientId+"] ...");
        var result:Transaction = null;
        try{
            transactionsLock.lock();
            result = transactions.getOrThrow(TRANS_ACTIVE).transMap.getOrElse(transId,null);
            if (result == null) {
                val x = transactions.getOrThrow(TRANS_WAITING).transMap.getOrElse(transId,null);
                val y = transactions.getOrThrow(TRANS_COMMITED).transMap.getOrElse(transId,null);
                val z = transactions.getOrThrow(TRANS_ABORTED).transMap.getOrElse(transId,null);
                
                if (x == null && y == null && z == null) {
                    result = new Transaction(transId,Timer.milliTime(), clientId);
                    transactions.getOrThrow(TRANS_ACTIVE).transMap.put(transId,result);
                    if (!timerOn) {
                    	timerOn = true;
                        async checkDeadTransCoordinator();
                    }
                }
            }
            if (result != null && !Utils.isTwoPhaseCommit()) {
            	result.replicas.addAll(replicas);
            }
        }
        finally {
            transactionsLock.unlock();
        }
        return result;
    }
    
    private def getTransactionLog(status:Int, transId:Long):Transaction {
        var result:Transaction = null;
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
        }finally {
            partitionsLock.unlock();
        }
    }
    
    //assumes the current transaction is Active
    private def readyToCommit(mapName:String, transLog:Transaction):Boolean {
        var ready:Boolean = true;
        try{
            transactionsLock.lock();
            
            if (!conflictingWithWaitingTransaction(transLog)
                && sameInitialVersions(mapName, transLog)
                && !conflictingWithMigratingPartition(transLog)) {
                val conflictReport = getConflictingActiveTransactions(transLog);
                if (conflictReport != null) {
                    if (conflictReport.maxTransId == transLog.transId) {
                        if (VERBOSE) Utils.console(moduleName, "TransId["+transLog.transId+"] Abort other transaction and become ready to commit ...");
                        //    abort other transactions
                        for (otherTransId in conflictReport.otherTransactions) {
                            val curTrans = transactions.getOrThrow(TRANS_ACTIVE).transMap.remove(otherTransId.transId);
                            transactions.getOrThrow(TRANS_ABORTED).transMap.put(otherTransId.transId, DUMMY_TRANSACTION);
                        }
                    }
                    else {
                        if (VERBOSE) Utils.console(moduleName, "TransId["+transLog.transId+"] Abort my self, NOT ready to commit ...");
                        // abort my self
                        ready = false;
                        val curTrans = transactions.getOrThrow(TRANS_ACTIVE).transMap.remove(transLog.transId);
                        transactions.getOrThrow(TRANS_ABORTED).transMap.put(transLog.transId, DUMMY_TRANSACTION);
                    }
                }
            }
            else{
                ready = false;
            }
            
            if (ready){
                val myTrans = transactions.getOrThrow(TRANS_ACTIVE).transMap.remove(transLog.transId);
                transactions.getOrThrow(TRANS_WAITING).transMap.put(transLog.transId, myTrans);
                if (!timerOn) {
                	timerOn = true;
                    async checkDeadTransCoordinator();
                }
            }
        }finally {
            transactionsLock.unlock();
        }
        return ready;
    }
    
    private def conflictingWithWaitingTransaction(transLog:Transaction):Boolean {
        var result:Boolean = false;
        val readyTransMap = transactions.getOrThrow(TRANS_WAITING).transMap;
        val iter = readyTransMap.keySet().iterator();
        while (iter.hasNext()) {
            val otherTransId = iter.next();
            val otherTrans = readyTransMap.getOrThrow(otherTransId);
            if (transLog.isConflicting(otherTrans)) {
                if (VERBOSE) Utils.console(moduleName, "Found conflict between transaction["+transLog.transId+"]  and ["+otherTrans.transId+"] ...");
                result = true;
                break;
            }
        }
        return result;
    }
    
    /* 
     * This function checks for conflicts with COMMITED transactions
     * If another transaction commited after my transaction started, 
     * the current values can be different from the values read by my transaction.
     * */
    private def sameInitialVersions(mapName:String, transLog:Transaction):Boolean {
        var result:Boolean = true;
        val map = transLog.getKeysCache();
        val iter = map.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val log = map.get(key);
            val oldVersion = log.getInitialVersion();
            
            val verValue = getVersionValue(log.getPartitionId(), mapName, key);
            
            var currentVersion:Int = -1n;
            if (verValue != null)
                currentVersion = verValue.getVersion();
            
            if (currentVersion != oldVersion) {
                if (VERBOSE) Utils.console(moduleName, "Version conflict transaction["+transLog.transId+"] key["+key+"] oldVersion["+oldVersion+"] newVersion["+currentVersion+"] ...");
                result = false;
                break;
            }
        }
        return result;
    }
    
    private def getConflictingActiveTransactions(transLog:Transaction):ConflictReport {
        val conflictList = new ArrayList[Transaction]();
        var maxTransId:Long = transLog.transId;
        val activeTransMap = transactions.getOrThrow(TRANS_ACTIVE).transMap;
        val iter = activeTransMap.keySet().iterator();
        while (iter.hasNext()) {
            val otherTransId = iter.next();
            val otherTrans = activeTransMap.getOrThrow(otherTransId);
            if (transLog.transId != otherTrans.transId && transLog.isConflicting(otherTrans)) {
                if (otherTrans.transId > maxTransId){
                    maxTransId = otherTrans.transId;
                }
                conflictList.add(otherTrans);                
            }
        }
        if (conflictList.size() == 0) {
            if (VERBOSE) Utils.console(moduleName, "ConflictReport for transaction["+transLog.transId+"] is null");
            return null;
        }
        else {
            val result = new ConflictReport(conflictList, maxTransId);
            if (VERBOSE) Utils.console(moduleName, "ConflictReport for transaction["+transLog.transId+"] is::: " + result.toString());
            return result;
        }
            
    }
    
    private def conflictingWithMigratingPartition(transLog:Transaction):Boolean {
        var result:Boolean = false;
        for (partitionId in migratingPartitions) { 
            if (transLog.isPartitionUsedForUpdate(partitionId)) {
                result = true;
                break;
            }
        }
        return result;
    }
    
    /**
     * Check if any of a transaction coordinator is dead.
     * In 2-phase commit:
     *      Active -> Abort
     *      Waiting -> Block!!  (mark the data store as invalid)
     *      
     * In 3-phase commit:
     *      Active -> Abort
     *      Waiting -> Elect a new coordinator to complete the transaction     * 
     **/
    public def checkDeadTransCoordinator() {
        while (timerOn) {
            System.threadSleep(100);
            if (VERBOSE) Utils.console(moduleName, "checkDeadTransCoordinator new iteration ...");
            try{
                transactionsLock.lock();
                /**Active transactions (same behaviour for 2-PC and 3-PC) **/
                val activeTrans =  transactions.getOrThrow(TRANS_ACTIVE).transMap;
                //activeTrans.printKeys();
                val iter = activeTrans.keySet().iterator();
                while (iter.hasNext()) {
                    val transId = iter.next();
                    val curTrans = activeTrans.getOrThrow(transId);                    
                    if (VERBOSE) Utils.console(moduleName, "check transaction client  TransId=["+transId+"] ClientPlace ["+curTrans.clientPlaceId+"] isDead["+Place(curTrans.clientPlaceId).isDead()+"] ...");
                    
                    if (Place(curTrans.clientPlaceId).isDead()) {                        
                        transactions.getOrThrow(TRANS_ACTIVE).transMap.remove(transId);
                        transactions.getOrThrow(TRANS_ABORTED).transMap.put(transId, DUMMY_TRANSACTION);
                        if (VERBOSE) Utils.console(moduleName, "Aborting transaction ["+transId+"] because client ["+Place(curTrans.clientPlaceId)+"] died ...");
                    }
                }
                
               
                /**Waiting transactions (2-PC block, 3-PC elect new coordinator) **/
                val waitingTrans =  transactions.getOrThrow(TRANS_WAITING).transMap;               
                val iterWaiting = waitingTrans.keySet().iterator();
                while (iterWaiting.hasNext()) {
                    val transId = iterWaiting.next();
                    
                    //check if we already detected a dead coordinator for this transaction
                    if (notifiedTransactions.contains(transId))
                    	continue;
                   
                    val curTrans = waitingTrans.getOrThrow(transId);                    
                    if (VERBOSE) Utils.console(moduleName, "check transaction client  TransId=["+transId+"] ClientPlace ["+curTrans.clientPlaceId+"] isDead["+Place(curTrans.clientPlaceId).isDead()+"] ...");
                    if (Place(curTrans.clientPlaceId).isDead()) {
                    	if (Utils.isTwoPhaseCommit ()) {
                    		transactions.getOrThrow(TRANS_WAITING).transMap.remove(transId);
                    		transactions.getOrThrow(TRANS_BLOCKED).transMap.put(transId, DUMMY_TRANSACTION);
                    		if (VERBOSE) Utils.console(moduleName, "Transaction ["+transId+"] BLOCKED because client ["+Place(curTrans.clientPlaceId)+"] died ...");
                    		DataStore.getInstance().invalidate();
                    		timerOn = false;
                    		break;
                    	}
                    	else {
                    		notifiedTransactions.add(transId);
                        	async DataStore.getInstance().leaderCoordinateTransaction(transId, curTrans.replicas);
                        }
                    }
                }
                
                /**Waiting transactions (2-PC block, 3-PC elect new coordinator) **/
                val preCommitTrans =  transactions.getOrThrow(TRANS_PRE_COMMIT).transMap;               
                val iterPreCommit = preCommitTrans.keySet().iterator();
                while (iterPreCommit.hasNext()) {
                    val transId = iterPreCommit.next();
                    
                    //check if we already detected a dead coordinator for this transaction
                    if (notifiedTransactions.contains(transId))
                    	continue;
                   
                    val curTrans = preCommitTrans.getOrThrow(transId);                    
                    if (VERBOSE) Utils.console(moduleName, "check transaction client  TransId=["+transId+"] ClientPlace ["+curTrans.clientPlaceId+"] isDead["+Place(curTrans.clientPlaceId).isDead()+"] ...");
                    if (Place(curTrans.clientPlaceId).isDead()) {                    	
                    	notifiedTransactions.add(transId);
                        async DataStore.getInstance().leaderCoordinateTransaction(transId, curTrans.replicas);                        
                    }
                }
                 
                val readyTransMap = transactions.getOrThrow(TRANS_WAITING).transMap;
                val activeTransMap = transactions.getOrThrow(TRANS_ACTIVE).transMap;
                if (readyTransMap.size() == 0 && activeTransMap.size() == 0){
                    timerOn = false;
                }
            }
            finally{
                transactionsLock.unlock();
            }
        }
    }
       
    /**
     * Migrating a partition to here
     **/
    public def addPartition(id:Long, maps:HashMap[String, HashMap[Any,VersionValue]]) {
        if (VERBOSE) Utils.console(moduleName, "Adding partition["+id+"] , waiting for partitions lock ...");
        try{
            partitionsLock.lock();
            val partition = new Partition(id, maps);
            partitions.put(id, partition);
        }
        finally{
            partitionsLock.unlock();
        }
        if (VERBOSE) Utils.console(moduleName, "Adding partition["+id+"] succeeded ...");
    }
    
    public def copyPartitionsTo(partitionId:Long, destPlaces:HashSet[Long], gr:GlobalRef[MigrationRequest]) {
        if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo partitionId["+partitionId+"] to places ["+Utils.hashSetToString(destPlaces)+"] ...");
        
        var partition:Partition = null;
        try{
        	partitionsLock.lock();
        	partition = partitions.getOrThrow(partitionId);
        } finally {
            partitionsLock.unlock();
        } 
        if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo - obtained reference to partition["+partitionId+"] ...");
        
        do {
        	var conflict:Boolean = false;
        	try{
        		transactionsLock.lock();
        		if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo - obtained transactionsLock ...");
        		conflict = isPartitionUsedForUpdate(partitionId);
        		if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo - conflict = "+conflict+" ...");
        		if (!conflict) {
        			migratingPartitions.add(partitionId);
        			if (VERBOSE) Utils.console(moduleName, "Adding partition ["+partitionId+"] to migList ... ");
        		}
        	}
        	finally{
        		transactionsLock.unlock();
        	}
        	
            if (!conflict) {
                val maps =  partition.getMaps();
                try{
                	finish for (placeId in destPlaces)	{
                		if (!Place(placeId).isDead()) {
                			at (Place(placeId)) async {
                				//NOTE: using verbose causes this to hang
                				DataStore.getInstance().getReplica().addPartition(partitionId, maps);
                			}
                		}
                	}
                	
                	at (gr.home) async {
                    	gr().complete();
                    }
                }catch(ex:Exception) {
                	ex.printStackTrace();
                }
                
                if (VERBOSE) Utils.console(moduleName, "Removing partition ["+partitionId+"] from migList ... ");
                try {
                    transactionsLock.lock();
                    migratingPartitions.remove(partitionId);
                }
                finally {
                    transactionsLock.unlock();
                }
                if (VERBOSE) Utils.console(moduleName, "Removing partition ["+partitionId+"] from migList SUCCEEDED ...");
                
                break;
            }
            else{
                if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo - partition is locked for a prepared update commit - WILL TRY AGAIN LATER...");
            }
            
             System.threadSleep(25);
        }
        while(true);
        
        if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo partitionId["+partitionId+"] to places ["+Utils.hashSetToString(destPlaces)+"] succeeded.  Broke infinite loop...");
        
    }
    
    /**
     * Returns true if the partition is being 'used for update' by an active or a readyToCommit transaction
     **/
    private def isPartitionUsedForUpdate(partitionId:Long):Boolean { 
    	val activeAndReadyToCommit = combineActiveAndReadyToCommitTransactions();
        val transMap = activeAndReadyToCommit.transMap;
        val iter = transMap.keySet().iterator();
        while (iter.hasNext()) {
            val transId = iter.next();
            val curTrans = transMap.getOrThrow(transId);
            if (curTrans.isPartitionUsedForUpdate(partitionId))
                true;
        }
        return false;
    }
    
    private def combineActiveAndReadyToCommitTransactions():TransactionsMap {
    	val combined = new TransactionsMap();
    	combined.addAll(transactions.getOrThrow(TRANS_ACTIVE));
    	combined.addAll(transactions.getOrThrow(TRANS_WAITING));
    	return combined;
    }

}

class TransactionsMap {
    public val transMap:HashMap[Long,Transaction] = new HashMap[Long,Transaction]();
    public def addAll(other:TransactionsMap) {
    	val iter = other.transMap.keySet().iterator();
    	while (iter.hasNext()) {
    		val id = iter.next();
    		val transaction = other.transMap.getOrThrow(id);
    		transMap.put(id, transaction);
    	}
    }
    
    public def printKeys() {
    	val iter = transMap.keySet().iterator();
    	var str:String = "";
    	while (iter.hasNext()) {
    		str += iter.next() + " , ";
    	}
    	Utils.console("TransactionsMap", "Combined keys = " + str);
    }
}

class ConflictReport {
    public val otherTransactions:ArrayList[Transaction];
    public val maxTransId:Long;
    public def this(list:ArrayList[Transaction], maxId:Long) {
        this.otherTransactions = list;
        this.maxTransId = maxId;
    }
    public def toString():String {
        var str:String = "";
        str += "ConflictReport with active transactions: ";
        for (x in otherTransactions)
            str += x.transId + ",";
        str += "   and maxTransId is: " + maxTransId;
        return str; 
    }
}