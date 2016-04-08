import x10.xrx.Runtime;
import x10.util.ArrayList;
import x10.compiler.NoInline;
import x10.util.HashSet;


public class ResilientMapImpl implements ResilientMap {
	private val moduleName = "ResilientMapImpl";
	public static val VERBOSE = Utils.getEnvLong("MAP_IMPL_VERBOSE", 0) == 1;
	
	
	private val name:String;
    private val timeoutMillis:Long;
    private val pendingRequests:ArrayList[MapRequest];
    private val executor:ReplicationManager;

    def this(name:String, timeoutMillis:Long, replicationManager:ReplicationManager) {
    	this.name = name;
    	this.timeoutMillis = timeoutMillis;
    	this.executor = replicationManager;
    	this.pendingRequests = new ArrayList[MapRequest]();
    }
    
    /**
     * Get value associated with a key
     * returns the current value
     **/
    public def get(key:Any):Any {
    	val txId = startTransaction();
    	var result:Any = null;
    	try{
    		result = get(txId, key);
    		commitTransaction(txId);
    	}catch(ex:Exception) {
    		abortTransaction(txId);
    		throw new TransactionAbortedException();
    	}
    	return result;
    }
    
    /**
     * Adds/updates value of given key
     * returns the old value
     **/
    public def put(key:Any, value:Any):Any {
    	val txId = startTransaction();
    	var oldValue:Any = null;
    	try{
    		oldValue = put(txId, key, value);
    		commitTransaction(txId);
    	}catch(ex:Exception) {
    		abortTransaction(txId);
    		throw new TransactionAbortedException();
    	}
    	return oldValue;
    }
    
    /**
     * Deletes the value of the given key
     * returns the old value
     **/
    public def delete(key:Any):Any {
    	val txId = startTransaction();
    	var oldValue:Any = null;
    	try{
    		oldValue = delete(txId, key);
    		commitTransaction(txId);
    	}catch(ex:Exception) {
    		abortTransaction(txId);
    		throw new TransactionAbortedException();
    	}
    	return oldValue;
    }
    
    
    /**
     * returns all keys
     **/
    public def keySet():HashSet[Any] {
    	val txId = startTransaction();
    	var result:HashSet[Any] = null;
    	try{
    		result = keySet(txId);
    		commitTransaction(txId);
    	}catch(ex:Exception) {
    		abortTransaction(txId);
    		throw new TransactionAbortedException();
    	}
    	return result;
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
    	val request = new MapRequest(transId, MapRequest.REQ_COMMIT);
    	executor.asyncExecuteRequest(name, request, timeoutMillis);   
    	if (VERBOSE) Utils.console(moduleName, "commitTransaction  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "commitTransaction  ... released } ");
    }
    
    /***
     * throws an exception if rollback failed  (this should not fail)
     */
    public def abortTransaction(transId:Long) {
    	val request = new MapRequest(transId, MapRequest.REQ_ABORT);
    	executor.asyncExecuteRequest(name, request, timeoutMillis);    	
    	if (VERBOSE) Utils.console(moduleName, "abortTransaction  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "abortTransaction  ... released } ");
    }
    
    /**
     * 
     * [Transaction] Get value associated with a key
     * returns the current value
     **/
    public def get(transId:Long, key:Any):Any {
    	val request = new MapRequest(transId, MapRequest.REQ_GET);
    	request.inKey = key;
    	executor.asyncExecuteRequest(name, request, timeoutMillis);    	
    	if (VERBOSE) Utils.console(moduleName, "get  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "get  ... released } ");
    	if (request.success)
    		return request.outValue;
    	else
    		throw request.outException;
    }
    
    /**
     * [Transaction] Adds/updates value of given key
     * returns the old value
     **/
    public def put(transId:Long, key:Any, value:Any):Any {
    	val request = new MapRequest(transId, MapRequest.REQ_PUT);
    	request.inKey = key;
    	request.inValue = value;
    	executor.asyncExecuteRequest(name, request, timeoutMillis);    	
    	if (VERBOSE) Utils.console(moduleName, "put  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "put  ... released } ");
    	if (request.success)
    		return request.outValue;
    	else
    		throw request.outException;
    }
    
    /**
     * [Transaction] Deletes the value of the given key
     * returns the old value
     **/
    public def delete(transId:Long,key:Any):Any {
    	val request = new MapRequest(transId, MapRequest.REQ_DELETE);
    	request.inKey = key;    	
    	executor.asyncExecuteRequest(name, request, timeoutMillis);    	
    	if (VERBOSE) Utils.console(moduleName, "delete  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "delete  ... released } ");
    	if (request.success)
    		return request.outValue;
    	else
    		throw request.outException;
    }
    
    
    /**
     * [Transaction] 
     * returns all keys
     **/
    public def keySet(transId:Long):HashSet[Any] {
    	val request = new MapRequest(transId, MapRequest.REQ_KEY_SET);    	
    	executor.asyncExecuteRequest(name, request, timeoutMillis);    	
    	if (VERBOSE) Utils.console(moduleName, "keySet  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "keySet  ... released } ");
    	if (request.success)
    		return request.outKeySet;
    	else
    		throw request.outException;
    }
}