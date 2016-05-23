package x10.util.resilient.map;

import x10.xrx.Runtime;
import x10.util.ArrayList;
import x10.compiler.NoInline;
import x10.util.HashSet;
import x10.util.resilient.map.common.Utils;

//The interface for the application usage
//Object can only be created by the DataStore class
public interface ResilientMap {
    
    /**
     * Get value associated with a key
     * returns the current value
     **/
    public def get(key:Any):Any;
    
    /**
     * Adds/updates value of given key
     * returns the old value
     **/
    public def put(key:Any, value:Any):Any;
    
    /**
     * Deletes the value of the given key
     * returns the old value
     **/
    public def delete(key:Any):Any;
    
    /***
     * returns the transactoin id
     */
    public def startTransaction():Long;
    
    /***
     * throws an exception if commit failed
     */
    public def commitTransaction(transId:Long):void;
    
    /**
     * aborts a transaction, exceptions are hiden
     */
    public def abortTransaction(transId:Long):void;
    
    
    /**
     * aborts a transaction and sleep for a random time that is less than (ABORT_SLEEP_MILLIS_MAX)
     */
    public def abortTransactionAndSleep(transId:Long):void;
    
    /**
     * 
     * [Transaction] Get value associated with a key
     * returns the current value
     **/
    public def get(transId:Long, key:Any):Any;
    
    /**
     * [Transaction] Adds/updates value of given key
     * returns the old value
     **/
    public def put(transId:Long, key:Any, value:Any):Any;
    
    /**
     * [Transaction] Deletes the value of the given key
     * returns the old value
     **/
    public def delete(transId:Long,key:Any):Any;
    
    
    /**
     * Returns the maximum number of allowed trials when a transaction fails
     * */
    public def retryMaximum():Long;
        
}