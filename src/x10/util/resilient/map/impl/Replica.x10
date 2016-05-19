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
    public static val TRANS_READY_TO_COMMIT:Int = 1n;
    public static val TRANS_COMMITED:Int = 2n;
    public static val TRANS_ABORTED:Int = 3n;
    
    public static val READY_YES:Long = 1;
    public static val READY_NO:Long = 0;
    
    // Dummy transaction log used to replace aborted/commited transactions
    private static val DUMMY_TRANSACTION = new Transaction(-1, -1, -1); 
    
    //transaction_status::transactions
    private val transactions:HashMap[Int,TransactionsMap];
    private val transactionsLock:SimpleLatch;
    
    private val partitions:HashMap[Long,Partition];
    private val partitionsLock:SimpleLatch;
    
    private var timerOn:Boolean = false;
    
    public def this(partitionIds:HashSet[Long]) {
        partitions = new HashMap[Long,Partition]();
        createPartitions(partitionIds);
        transactionsLock = new SimpleLatch();
        partitionsLock = new SimpleLatch();
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
    
    
    public def submitSingleKeyRequest(mapName:String, clientId:Long, partitionId:Long, transId:Long, requestType:Int, key:Any, value:Any, responseGR:GlobalRef[MapRequest]) {
         switch(requestType) {
            case MapRequest.REQ_GET: get(mapName, clientId, partitionId, transId, key, value, responseGR); break;
            case MapRequest.REQ_PUT: put(mapName, clientId, partitionId, transId, key, value, responseGR); break;
            case MapRequest.REQ_DELETE: delete(mapName, clientId, partitionId, transId, key, responseGR); break;
        }
    }
    
    public def get(mapName:String, clientId:Long, partitionId:Long, transId:Long, key:Any, value:Any, responseGR:GlobalRef[MapRequest]) {
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
    
    public def put(mapName:String, clientId:Long, partitionId:Long, transId:Long, key:Any, value:Any, responseGR:GlobalRef[MapRequest]) {        
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
    
    public def delete(mapName:String, clientId:Long, partitionId:Long, transId:Long, key:Any, responseGR:GlobalRef[MapRequest]) {
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
        val transLog = getTransactionLog(TRANS_READY_TO_COMMIT, transId);
        val cache = transLog.getKeysCache();
        val keysIter = cache.keySet().iterator();

        while (keysIter.hasNext()) {
            val key = keysIter.next();
            val log = cache.getOrThrow(key);
            if (log.readOnly()) continue;
            
            try{
                partitionsLock.lock();
                val partition = partitions.getOrThrow(log.getPartitionId());
                if (VERBOSE) Utils.console(moduleName, "TransId["+transId+"] Applying commit changes:=> " + log.toString());
                if (log.isDeleted()) {
                    partition.delete(mapName, key);
                }
                else if (!log.readOnly()) {
                    partition.put(mapName, key, log.getValue());
                }
            }
            finally{
                partitionsLock.unlock();
            }
        }
        
        try{
            transactionsLock.lock();
            transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap.remove(transId);
            transactions.getOrThrow(TRANS_COMMITED).transMap.put(transId, DUMMY_TRANSACTION);
        }finally {
            transactionsLock.unlock();
        }
    }
    
    public def abortNoResponse(transId:Long, responseGR:GlobalRef[MapRequest]) {
        try{
            transactionsLock.lock();
            var transLog:Transaction = transactions.getOrThrow(TRANS_ACTIVE).transMap.remove(transId);
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
    
    
    private def getOrAddActiveTransaction(transId:Long, clientId:Long):Transaction {
        var result:Transaction = null;
        try{
            transactionsLock.lock();
            result = transactions.getOrThrow(TRANS_ACTIVE).transMap.getOrElse(transId,null);
            if (result == null) {
                val x = transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap.getOrElse(transId,null);
                val y = transactions.getOrThrow(TRANS_COMMITED).transMap.getOrElse(transId,null);
                val z = transactions.getOrThrow(TRANS_ABORTED).transMap.getOrElse(transId,null);
                
                if (x == null && y == null && z == null) {
                    result = new Transaction(transId,Timer.milliTime(), clientId);
                    transactions.getOrThrow(TRANS_ACTIVE).transMap.put(transId,result);
                    if (!timerOn)
                        timerOn = true;
                }
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
            
            if (!conflictingWithReadyTransaction(transLog)
                && sameInitialVersions(mapName, transLog)) {
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
                transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap.put(transLog.transId, myTrans);
                if (!timerOn)
                    async checkDeadClientForReadyTransactions();
            }
        }finally {
            transactionsLock.unlock();
        }
        return ready;
    }
    
    private def conflictingWithReadyTransaction(transLog:Transaction):Boolean {
        var result:Boolean = false;
        val readyTransMap = transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap;
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
    
    /**
     * Abort readyToCommit transactions if their client is dead
     * TODO: should also check active transactions
     **/
    public def checkDeadClientForReadyTransactions () {
        while (timerOn) {
            System.threadSleep(100);
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
    
    
    /**
     * Migrating a partition to here
     **/
    public def addPartition(id:Long, maps:HashMap[String, HashMap[Any,VersionValue]]) {
        if (VERBOSE) Utils.console(moduleName, "Adding partition , waiting for partitions lock ...");
        try{
            partitionsLock.lock();
            val partition = new Partition(id, maps);
            partitions.put(id, partition);
        }
        finally{
            partitionsLock.unlock();
        }
        if (VERBOSE) Utils.console(moduleName, "Adding partition succeeded ...");
    }
    
    public def copyPartitionsTo(partitionId:Long, destPlaces:HashSet[Long], gr:GlobalRef[MigrationRequest]) {
        if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo partitionId["+partitionId+"] to places ["+Utils.hashSetToString(destPlaces)+"] ...");
        do {
            try{
                transactionsLock.lock();
                if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo - obtained transactionsLock ...");
                if (!isPartitionUsedForUpdate(partitionId)) {
                    partitionsLock.lock();
                    try{
                        val partition = partitions.getOrThrow(partitionId);
                        if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo - partition ready to migrate ...");
                        
                        val maps =  partition.getMaps();
                        finish for (placeId in destPlaces) at (Place(placeId)) async {
                            //NOTE: using verbose causes this to hang
                            DataStore.getInstance().getReplica().addPartition(partitionId, maps);
                        }
                    
                        at (gr.home) {
                            gr().complete();
                        }
                    }finally {
                        partitionsLock.unlock();
                    }
                    break;
                }
                else{
                    if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo - partition is locked for a prepared update commit - WILL TRY AGAIN ...");
                }
            }
            finally{
                transactionsLock.unlock();
            }
            
             System.threadSleep(10);
        }
        while(true);
        
        if (VERBOSE) Utils.console(moduleName, "copyPartitionsTo partitionId["+partitionId+"] to places ["+Utils.hashSetToString(destPlaces)+"] succeeded.  Broke infinite loop...");
        
    }
    
    /**
     * Returns true if the partition is being 'used for update' by an active or a readyToCommit transaction
     **/
    private def isPartitionUsedForUpdate(partitionId:Long):Boolean { 
        val readyTransMap = transactions.getOrThrow(TRANS_READY_TO_COMMIT).transMap;
        val iter1 = readyTransMap.keySet().iterator();
        while (iter1.hasNext()) {
            val transId = iter1.next();
            val curTrans = readyTransMap.getOrThrow(transId);
            if (curTrans.isPartitionUsedForUpdate(partitionId))
                true;
        }
        
        val activeTransMap = transactions.getOrThrow(TRANS_ACTIVE).transMap;
        val iter2 = activeTransMap.keySet().iterator();
        while (iter2.hasNext()) {
            val transId = iter2.next();
            val curTrans = activeTransMap.getOrThrow(transId);
            if (curTrans.isPartitionUsedForUpdate(partitionId))
                true;
        }      
        
        return false;
    }
    
    public def toString():String {
        var str:String = "Primary [";
        
        str += "]\n";
        return str;
    }
}

class TransactionsMap {
    public val transMap:HashMap[Long,Transaction] = new HashMap[Long,Transaction]();
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