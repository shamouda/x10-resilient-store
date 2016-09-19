package x10.util.resilient.map.transaction;

import x10.util.concurrent.SimpleLatch;
import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.resilient.map.DataStore;
import x10.util.resilient.map.impl.MapRequest;
import x10.util.resilient.map.common.Utils;
import x10.xrx.Runtime;
/*
 * Invoked when a transaction client dies, while the transaction is in 'Ready to Commit' 
 * state in at least one of the replicas.
 * In that case, the replica requests the leader to take over the transaction recovery procedure.
 * 
 * The functionality provided here is very similar to ReplicaClient
 **/
public class TransactionRecoveryManager {
    private val moduleName = "TransactionRecoveryManager";
    public static val VERBOSE = Utils.getEnvLong("TRANS_RECOVERY_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    
    private val pendingRequests = new ArrayList[RecoveryRequest](); //transactions that need recovery
    private var processing:Boolean = false;
    private val lock = new SimpleLatch();
      
    private var valid:Boolean = true;
    
    public def addRequest(reportingReplica:Long, transactionId:Long, replicas:HashSet[Long]) {
        if (VERBOSE) Utils.console(moduleName, "adding recovery request from replica ["+reportingReplica+"]");
        try{
            lock.lock();
            var found:Boolean = false;
            for (req in pendingRequests) {
            	if (req.transactionId == transactionId) {
            		found = true;
            		break;
            	}
            }
            if (!found) {
            	pendingRequests.add(new RecoveryRequest(reportingReplica,transactionId, replicas));
            	if (!processing) {
            		if (VERBOSE) Utils.console(moduleName, "starting an async recovery thread ...");
            		processing = true;
            		async processRequests();
            	}
            }
        }finally {
            lock.unlock();
        }
    }
    
    public def processRequests() {
        if (VERBOSE) Utils.console(moduleName, "processing recovery requests ...");
        var nextReq:RecoveryRequest = nextRequest();
        Runtime.increaseParallelism();
        while(nextReq != null){
            if (VERBOSE) Utils.console(moduleName, "new iteration for processing recovery requests ...");
            
            val transId = nextReq.transactionId;
            val replicas = nextReq.replicas;
            
            async {
            	try{
            		commitTransaction(transId, replicas);
            	}catch (ex:Exception) {
            		abortTransaction(transId, replicas);
            	}
            }
            
            nextReq = nextRequest();
        }
        Runtime.decreaseParallelism(1n);
    }

    private def nextRequest():RecoveryRequest {
        var result:RecoveryRequest = null;
        try{
            lock.lock();
            if (pendingRequests.size() > 0) {                
                result = pendingRequests.removeAt(0);
                if (VERBOSE) Utils.console(moduleName, "next TransactionRecoveryRequest is: " + result.toString());
            }
            else
                processing = false;
        }
        finally{
            lock.unlock();
        }
        return result;
    }
    
    public def commitTransaction(transId:Long, replicas:HashSet[Long]) {        
        val request = new MapRequest(transId, MapRequest.REQ_PREPARE_AND_COMMIT, null);
        request.setReplicationInfo(replicas);
        request.enableCommitRecovery();
        DataStore.getInstance().executor().asyncExecuteRequest(request);
        if (VERBOSE) Utils.console(moduleName, "Recover_commitTransaction["+transId+"]  { await ... ");
        request.lock.await();
        if (VERBOSE) Utils.console(moduleName, "Recover_commitTransaction["+transId+"]          ... released }    Success="+request.isSuccessful());
        if (!request.isSuccessful())
            throw request.outException;
    }
    
    public def abortTransaction(transId:Long, replicas:HashSet[Long]) {
        val request = new MapRequest(transId, MapRequest.REQ_ABORT, null);
        request.setReplicationInfo(replicas);
        DataStore.getInstance().executor().asyncExecuteRequest(request);        
        if (VERBOSE) Utils.console(moduleName, "Recover_abortTransaction["+transId+"]  { await ... ");
        request.lock.await();
        if (VERBOSE) Utils.console(moduleName, "Recover_abortTransaction["+transId+"]          ... released }    Success="+request.isSuccessful());
    }
}

class RecoveryRequest (reportingReplica:Long, transactionId:Long, replicas:HashSet[Long]) {
	public def toString():String {
		var result:String = "reportingReplica=["+reportingReplica+"] transactionId["+transactionId+"] replicas[";
	    for (c in replicas)
	    	result += c + ",";	    
	    result += "]";
		return result;
	}
	
}