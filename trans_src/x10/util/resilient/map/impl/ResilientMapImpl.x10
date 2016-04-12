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
	public static val RETRY_MAX = Utils.getEnvLong("TRANS_RETRY_MAX", 3);
	
	private val name:String;
    private val timeoutMillis:Long;
    
    public def this(name:String, timeoutMillis:Long) {
    	this.name = name;
    	this.timeoutMillis = timeoutMillis;   
    }
    
    public def tryOperation(requestType:Int,key:Any, value:Any):Any {
    	var attempt:Long = 0;
    	var result:Any = null;
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
        		commitTransaction(txId);
        		break;
        	} catch(ex:Exception) {
        		ex.printStackTrace();
        		try {
        			
        			abortTransaction(txId);
        			
        		}catch(abortEx:Exception) {
        			abortEx.printStackTrace();
        		}
        	}
        	attempt ++;
        	System.sleep(10);
        } while (attempt < RETRY_MAX);
    	
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
    	val request = new MapRequest(transId, MapRequest.REQ_COMMIT, name, timeoutMillis);
    	//if (VERBOSE) Utils.console(moduleName, "THE COMMIT REQ = " + request.toString());
    	
    	DataStore.getInstance().executor().asyncExecuteRequest(request);   
    	if (VERBOSE) Utils.console(moduleName, "commitTransaction  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "commitTransaction          ... released } ");
    	if (!request.isSuccessful())
    		throw request.outException;
    	if (VERBOSE) Utils.console(moduleName, "commitTransaction SUCCEEDED ...");
    }
    
    /***
     * throws an exception if rollback failed  (this should not fail)
     */
    public def abortTransaction(transId:Long) {
    	val request = new MapRequest(transId, MapRequest.REQ_ABORT, name, timeoutMillis);
    	DataStore.getInstance().executor().asyncExecuteRequest(request);    	
    	if (VERBOSE) Utils.console(moduleName, "abortTransaction  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "abortTransaction          ... released } ");
    	if (!request.isSuccessful())
    		throw request.outException;
    	if (VERBOSE) Utils.console(moduleName, "abortTransaction SUCCEEDED ...");
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
    	
    	if (VERBOSE) Utils.console(moduleName, "get  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "get          ... released } ");
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
    	if (VERBOSE) Utils.console(moduleName, "put  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "put          ... released } ");
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
    	if (VERBOSE) Utils.console(moduleName, "delete  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "delete          ... released } ");
    	if (request.isSuccessful())
    		return request.outValue;
    	else
    		throw request.outException;
    }
}