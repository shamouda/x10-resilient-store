import x10.xrx.Runtime;
import x10.util.ArrayList;
import x10.compiler.NoInline;
import x10.util.HashSet;


//Object can only be created by the DataStore class
public class ResilientMapImpl implements ResilientMap {
	private val name:String;

    def this(name:String) {
    	this.name = name;    	
    }
    
    /**
     * Get value associated with a key
     * returns the current value
     **/
    public def get(key:Any):Any {
    	return null;
    }
    
    /**
     * Adds/updates value of given key
     * returns the old value
     **/
    public def put(key:Any, value:Any):Any {
        return null;
    }
    
    /**
     * Deletes the value of the given key
     * returns the old value
     **/
    public def delete(key:Any):Any {
    	return null;
    }
    
    
    /**
     * returns all keys
     **/
    public def keySet():HashSet[Any] {
    	return null;
    }
    
    /***
     * returns the transactoin id
     */
    public def startTransaction():Long {
    	return -1;
    }
    
    /***
     * throws an exception if commit failed
     */
    public def commitTransaction(transId:Long) {
    	
    }
    
    /***
     * throws an exception if rollback failed  (this should not fail)
     */
    public def rollbackTransaction(transId:Long) {
    	
    }
    
    /**
     * 
     * [Transaction] Get value associated with a key
     * returns the current value
     **/
    public def get(transId:Long, key:Any):Any {
    	return null;
    }
    
    /**
     * [Transaction] Adds/updates value of given key
     * returns the old value
     **/
    public def put(transId:Long, key:Any, value:Any):Any {
        return null;
    }
    
    /**
     * [Transaction] Deletes the value of the given key
     * returns the old value
     **/
    public def delete(transId:Long,key:Any):Any {
    	return null;
    }
    
    
    /**
     * [Transaction] 
     * returns all keys
     **/
    public def keySet(transId:Long):HashSet[Any] {
    	return null;
    }


    
}