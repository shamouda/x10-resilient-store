import x10.xrx.Runtime;
import x10.util.ArrayList;
import x10.compiler.NoInline;
import x10.util.HashSet;

/***
 * This class should not contain heavy objects because it is transferable between places
 * Place specific data should be obtained from the DataStore class
 * */
public class ResilientMapImpl implements ResilientMap {
	private val moduleName = "ResilientMapImpl";
	public static val VERBOSE = Utils.getEnvLong("MAP_IMPL_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
	
	
	private val name:String;
    private val timeoutMillis:Long;

    def this(name:String, timeoutMillis:Long) {
    	this.name = name;
    	this.timeoutMillis = timeoutMillis;    	
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
    		ex.printStackTrace();
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
    		ex.printStackTrace();
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
    		ex.printStackTrace();
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
    		ex.printStackTrace();
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
    	DataStore.getInstance().executor().asyncExecuteRequest(name, request, timeoutMillis);   
    	if (VERBOSE) Utils.console(moduleName, "commitTransaction  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "commitTransaction  ... released } ");
    }
    
    /***
     * throws an exception if rollback failed  (this should not fail)
     */
    public def abortTransaction(transId:Long) {
    	val request = new MapRequest(transId, MapRequest.REQ_ABORT);
    	DataStore.getInstance().executor().asyncExecuteRequest(name, request, timeoutMillis);    	
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
    	
    	async DataStore.getInstance().executor().asyncExecuteRequest(name, request, timeoutMillis);
    	
    	if (VERBOSE) Utils.console(moduleName, "get  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "get  ... released } ");
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
    	val request = new MapRequest(transId, MapRequest.REQ_PUT);
    	request.inKey = key;
    	request.inValue = value;
    	DataStore.getInstance().executor().asyncExecuteRequest(name, request, timeoutMillis);    	
    	if (VERBOSE) Utils.console(moduleName, "put  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "put  ... released } ");
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
    	val request = new MapRequest(transId, MapRequest.REQ_DELETE);
    	request.inKey = key;    	
    	DataStore.getInstance().executor().asyncExecuteRequest(name, request, timeoutMillis);    	
    	if (VERBOSE) Utils.console(moduleName, "delete  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "delete  ... released } ");
    	if (request.isSuccessful())
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
    	DataStore.getInstance().executor().asyncExecuteRequest(name, request, timeoutMillis);    	
    	if (VERBOSE) Utils.console(moduleName, "keySet  { await ... ");
    	request.lock.await();
    	if (VERBOSE) Utils.console(moduleName, "keySet  ... released } ");
    	if (request.isSuccessful())
    		return request.outKeySet;
    	else
    		throw request.outException;
    }
}