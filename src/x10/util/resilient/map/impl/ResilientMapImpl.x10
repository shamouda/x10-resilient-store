package x10.util.resilient.map.impl;

import x10.xrx.Runtime;
import x10.util.ArrayList;
import x10.compiler.NoInline;
import x10.util.HashSet;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.exception.TransactionAbortedException;
import x10.util.resilient.map.ResilientMap;
import x10.util.resilient.map.DataStore;


/***
 * This class should not contain heavy objects because it is transferable between places
 * Place specific data should be obtained from the DataStore class
 * */
public class ResilientMapImpl implements ResilientMap {
    private val moduleName = "ResilientMapImpl";
    public static val VERBOSE = Utils.getEnvLong("MAP_IMPL_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    public static val RETRY_MAX = Utils.getEnvLong("TRANS_RETRY_MAX", Place.numPlaces());
    
    private val name:String;
    private val timeoutMillis:Long;
    
    public def this(name:String, timeoutMillis:Long) {
        this.name = name;
        this.timeoutMillis = timeoutMillis;   
    }
    
    public def retryMaximum() = RETRY_MAX;
    
    public def tryOperation(requestType:Int,key:Any, value:Any):Any {
        var attempt:Long = 0;
        var result:Any = null;
        var succeeded:Boolean = false;
        var commitException:Exception = null; 
        do {
            if (VERBOSE) Utils.console(moduleName,"$$= Running attempt ["+(attempt+1)+"/"+RETRY_MAX+"] for request ["+MapRequest.typeDesc(requestType)+"] =$$");
            val txId = startTransaction();
            try{
                if (requestType == MapRequest.REQ_GET) {
                    result = get(txId, key);
                } else if (requestType == MapRequest.REQ_PUT) {
                    result = put(txId, key, value);
                } else if (requestType == MapRequest.REQ_DELETE) {
                    result = delete(txId, key);
                }
                if (VERBOSE) Utils.console(moduleName, "Result = " + result);
                commitTransaction(txId);
                succeeded = true;
                break;
            } catch(ex:Exception) {
                commitException = ex;
                if (VERBOSE) ex.printStackTrace();
                try {
                    abortTransaction(txId);                    
                }catch(abortEx:Exception) {
                    if (VERBOSE) abortEx.printStackTrace();
                }
            }
            attempt ++;
            System.sleep(10);
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
    
    /***
     * throws an exception if commit failed
     */
    public def commitTransaction(transId:Long) {        
        val request = new MapRequest(transId, MapRequest.REQ_PREPARE_COMMIT, name, timeoutMillis);
        //if (VERBOSE) Utils.console(moduleName, "THE COMMIT REQ = " + request.toString());
        
        DataStore.getInstance().executor().asyncExecuteRequest(request);   
        if (VERBOSE) Utils.console(moduleName, "commitTransaction["+transId+"]  { await ... ");
        request.lock.await();
        if (VERBOSE) Utils.console(moduleName, "commitTransaction["+transId+"]          ... released }    Success="+request.isSuccessful());
        if (!request.isSuccessful())
            throw request.outException;
    }
    
    /***
     * throws an exception if rollback failed  (this should not fail)
     */
    public def abortTransaction(transId:Long) {
        val request = new MapRequest(transId, MapRequest.REQ_ABORT, name, timeoutMillis);
        DataStore.getInstance().executor().asyncExecuteRequest(request);        
        if (VERBOSE) Utils.console(moduleName, "abortTransaction["+transId+"]  { await ... ");
        request.lock.await();
        if (VERBOSE) Utils.console(moduleName, "abortTransaction["+transId+"]          ... released }    Success="+request.isSuccessful());
        /*
        We don't expect abort to throw an exception
        if (!request.isSuccessful())
            throw request.outException;
        */
    }
    
    /**
     * 
     * [Transaction] Get value associated with a key
     * returns the current value
     **/
    public def get(transId:Long, key:Any):Any {
        val request = new MapRequest(transId, MapRequest.REQ_GET, name, timeoutMillis);
        request.inKey = key;
        
        async DataStore.getInstance().executor().asyncExecuteRequest(request);
        
        if (VERBOSE) Utils.console(moduleName, "get["+transId+"]  { await ... ");
        request.lock.await();
        if (VERBOSE) Utils.console(moduleName, "get["+transId+"]          ... released }    Success="+request.isSuccessful());
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
        val request = new MapRequest(transId, MapRequest.REQ_PUT, name, timeoutMillis);
        request.inKey = key;
        request.inValue = value;
        DataStore.getInstance().executor().asyncExecuteRequest(request);        
        if (VERBOSE) Utils.console(moduleName, "put["+transId+"]  { await ... ");
        request.lock.await();
        if (VERBOSE) Utils.console(moduleName, "put["+transId+"]          ... released }    Success="+request.isSuccessful());
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
        val request = new MapRequest(transId, MapRequest.REQ_DELETE, name, timeoutMillis);
        request.inKey = key;        
        DataStore.getInstance().executor().asyncExecuteRequest(request);        
        if (VERBOSE) Utils.console(moduleName, "delete["+transId+"]  { await ... ");
        request.lock.await();
        if (VERBOSE) Utils.console(moduleName, "delete["+transId+"]          ... released }    Success="+request.isSuccessful());
        if (request.isSuccessful())
            return request.outValue;
        else
            throw request.outException;
    }
}