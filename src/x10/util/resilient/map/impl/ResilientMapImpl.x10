package x10.util.resilient.map.impl;

import x10.xrx.Runtime;
import x10.util.ArrayList;
import x10.compiler.NoInline;
import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.exception.TransactionAbortedException;
import x10.util.resilient.map.ResilientMap;
import x10.util.resilient.map.DataStore;
import x10.util.Random;
import x10.util.Timer;
import x10.xrx.Runtime;
import x10.compiler.Ifdef;
/***
 * This class should not contain heavy objects because it is transferable between places
 * Place specific data should be obtained from the DataStore class
 * */
public class ResilientMapImpl implements ResilientMap {
    private val moduleName = "ResilientMapImpl";
    public static val VERBOSE = Utils.getEnvLong("MAP_IMPL_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    public static val RETRY_MAX = Utils.getEnvLong("TRANS_RETRY_MAX", Place.numPlaces());
    
    public static val ABORT_SLEEP_MILLIS_MAX = Utils.getEnvLong("ABORT_SLEEP_MILLIS_MAX", 20);
    
    public val preparedCommitRequests:HashMap[Long,HashSet[Long]];
    
    private val name:String;
    private val rnd = new Random(Timer.milliTime());
    
    public def this(name:String) {    	
        this.name = name;   
        this.preparedCommitRequests = new HashMap[Long,HashSet[Long]]();
    }
    
    public def retryMaximum() = RETRY_MAX;
    
    public def isValid() = DataStore.getInstance().isValid();
    
    public def tryOperation(requestType:Int,key:Any, value:Any):Any {
        var attempt:Long = 0;
        var result:Any = null;
        var succeeded:Boolean = false;
        var commitException:Exception = null; 
        do {
        	@Ifdef("__DS_DEBUG__") { Utils.console(moduleName,"$$= Running attempt ["+(attempt+1)+"/"+RETRY_MAX+"] for request ["+MapRequest.typeDesc(requestType)+"] =$$"); }
            val txId = startTransaction();
            try{
                if (requestType == MapRequest.REQ_GET) {
                    result = get(txId, key);
                } else if (requestType == MapRequest.REQ_PUT) {
                    result = put(txId, key, value);
                } else if (requestType == MapRequest.REQ_DELETE) {
                    result = delete(txId, key);
                }
                @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "Result = " + result); }
                commitTransaction(txId);
                succeeded = true;
                break;
            } catch(ex:Exception) {
                commitException = ex;
                if (VERBOSE) ex.printStackTrace();
                    abortTransaction(txId);                    
            }
            attempt ++;
        } while (attempt < RETRY_MAX);
        
        if (!succeeded)
            throw commitException;
        return result;
    }
    
    /**
     * Get value associated with a key
     * returns the current value
     **/
    public def get(key:Any):Any {
        return tryOperation(MapRequest.REQ_GET, key, null);
    }
    
    
    /**
     * Adds/updates value of given key
     * returns the old value
     **/
    public def put(key:Any, value:Any):Any {
        return tryOperation(MapRequest.REQ_PUT, key, value);
    }
    
    /**
     * Deletes the value of the given key
     * returns the old value
     **/
    public def delete(key:Any):Any {
        return tryOperation(MapRequest.REQ_DELETE, key, null);
    }
    
    /***
     * returns the transactoin id
     */
    public def startTransaction():Long {
        return Utils.getNextTransactionId();
    }
    
    public def prepareCommit(transId:Long):Boolean {
    	val request = new MapRequest(transId, MapRequest.REQ_PREPARE_ONLY, name);
        DataStore.getInstance().executor().asyncExecuteRequest(request);   
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "prepareCommit["+transId+"]  { await ... "); }
        Runtime.increaseParallelism();
        request.lock.await();
        Runtime.decreaseParallelism(1n);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "prepareCommit["+transId+"]          ... released }    Success="+request.isSuccessful()); }
        
        preparedCommitRequests.put(transId, request.replicas);
        if (request.isSuccessful()) {
            if ( (request.outValue as Int) == MapRequest.CONFIRM_COMMIT)
                return true;
            else
                return false;
        }
        else
            throw request.outException;
    }
    
    public def confirmCommit(transId:Long) {
    	val request = new MapRequest(transId, MapRequest.REQ_CONFIRM_COMMIT, name);
    	val replicas = preparedCommitRequests.remove(transId);
    	request.replicas = replicas;
        DataStore.getInstance().executor().asyncExecuteRequest(request);   
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "confirmCommit["+transId+"]  { await ... "); }
        Runtime.increaseParallelism();
        request.lock.await();
        Runtime.decreaseParallelism(1n);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "confirmCommit["+transId+"]          ... released }    Success="+request.isSuccessful()); }
        if (!request.isSuccessful())
            throw request.outException;
    }
    /***
     * throws an exception if commit failed
     */
    public def commitTransaction(transId:Long) {        
        val request = new MapRequest(transId, MapRequest.REQ_PREPARE_AND_COMMIT, name);
        DataStore.getInstance().executor().asyncExecuteRequest(request);   
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "prepareAndCommit["+transId+"]  { await ... "); }
        Runtime.increaseParallelism();
        request.lock.await();
        Runtime.decreaseParallelism(1n);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "prepareAndCommit["+transId+"]          ... released }    Success="+request.isSuccessful()); }
        if (!request.isSuccessful())
            throw request.outException;
    }
    
    /***
     * No exceptions thrown
     */
    public def abortTransaction(transId:Long) {
        val request = new MapRequest(transId, MapRequest.REQ_ABORT, name);
        DataStore.getInstance().executor().asyncExecuteRequest(request);        
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "abortTransaction["+transId+"]  { await ... "); }
        Runtime.increaseParallelism();
        request.lock.await();
        Runtime.decreaseParallelism(1n);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "abortTransaction["+transId+"]          ... released }    Success="+request.isSuccessful()); }
    }
    
    
    /***
     * throws an exception if rollback failed  (this should not fail)
     */
    public def abortTransactionAndSleep(transId:Long) {
        val request = new MapRequest(transId, MapRequest.REQ_ABORT, name);
        DataStore.getInstance().executor().asyncExecuteRequest(request);        
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "abortTransaction["+transId+"]  { await ... "); }
        Runtime.increaseParallelism();
        request.lock.await();
        Runtime.decreaseParallelism(1n);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "abortTransaction["+transId+"]          ... released }    Success="+request.isSuccessful()); }
        nextRandomSleep();
    }
    
    
    /**
     * 
     * [Transaction] Get value associated with a key
     * returns the current value
     **/
    public def get(transId:Long, key:Any):Any {
        val request = new MapRequest(transId, MapRequest.REQ_GET, name);
        request.inKey = key;
        
        async DataStore.getInstance().executor().asyncExecuteRequest(request);
        
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "get["+transId+"]  { await ... "); }
        Runtime.increaseParallelism();
        request.lock.await();
        Runtime.decreaseParallelism(1n);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "get["+transId+"]          ... released }    Success="+request.isSuccessful()); }
        if (request.isSuccessful())
            return request.outValue;
        else
            throw request.outException;
    }
    
    /**
     * [Transaction] Adds/updates value of given key
     * returns the old value
     **/
    public def put(transId:Long, key:Any, value:Any):Any {
        val request = new MapRequest(transId, MapRequest.REQ_PUT, name);
        request.inKey = key;
        request.inValue = value;
        DataStore.getInstance().executor().asyncExecuteRequest(request);        
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "put["+transId+"]  { await ... "); }
        Runtime.increaseParallelism();
        request.lock.await();
        Runtime.decreaseParallelism(1n);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "put["+transId+"]          ... released }    Success="+request.isSuccessful()); }
        if (request.isSuccessful())
            return request.outValue;
        else
            throw request.outException;
    }
    
    /**
     * [Transaction] Deletes the value of the given key
     * returns the old value
     **/
    public def delete(transId:Long,key:Any):Any {
        val request = new MapRequest(transId, MapRequest.REQ_DELETE, name);
        request.inKey = key;        
        DataStore.getInstance().executor().asyncExecuteRequest(request);        
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "delete["+transId+"]  { await ... "); }
        Runtime.increaseParallelism();
        request.lock.await();
        Runtime.decreaseParallelism(1n);
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "delete["+transId+"]          ... released }    Success="+request.isSuccessful()); }
        if (request.isSuccessful())
            return request.outValue;
        else
            throw request.outException;
    }
    
    private def nextRandomSleep():Long {
        val time = Math.abs(rnd.nextLong()) % ABORT_SLEEP_MILLIS_MAX;
        @Ifdef("__DS_DEBUG__") { Utils.console(moduleName, "sleeping for " + time + "ms"); }
        return time;
    }
}