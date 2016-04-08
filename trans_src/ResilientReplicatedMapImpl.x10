import x10.xrx.Runtime;
import x10.util.ArrayList;
import x10.compiler.NoInline;
import x10.util.HashSet;


public class ResilientReplicatedMapImpl {
    
    /**
     * Get value associated with a key
     * returns the current value
     **/
    public def getInternal(key:Any):Any {
    	return null;
    }
    
    /**
     * Adds/updates value of given key
     * returns the old value
     **/
    public def putInternal(key:Any, value:Any):Any {
        return null;
    }
    
    /**
     * Deletes the value of the given key
     * returns the old value
     **/
    public def deleteInternal(key:Any):Any {
    	return null;
    }
    
    
    /**
     * returns all keys
     **/
    public def keySetInternal():HashSet[Any] {
    	return null;
    }
    
    /***
     * returns the transactoin id
     */
    public def startTransactionInternal():Long {
    	return -1;
    }
    
    /***
     * throws an exception if commit failed
     */
    public def commitTransactionInternal(transId:Long) {
    	
    }
    
    /***
     * throws an exception if rollback failed  (this should not fail)
     */
    public def rollbackTransactionInternal(transId:Long) {
    	
    }
    
    /**
     * 
     * [Transaction] Get value associated with a key
     * returns the current value
     **/
    public def getInternal(transId:Long, key:Any):Any {
    	return null;
    }
    
    /**
     * [Transaction] Adds/updates value of given key
     * returns the old value
     **/
    public def putInternal(transId:Long, key:Any, value:Any):Any {
        return null;
    }
    
    /**
     * [Transaction] Deletes the value of the given key
     * returns the old value
     **/
    public def deleteInternal(transId:Long,key:Any):Any {
    	return null;
    }
    
    
    /**
     * [Transaction] 
     * returns all keys
     **/
    public def keySetInternal(transId:Long):HashSet[Any] {
    	return null;
    }


    
}