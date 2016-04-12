package x10.util.resilient.map.transaction;

import x10.util.HashMap;
import x10.util.resilient.map.common.Utils;

public class TransCachedRecord {
	private val initVersion:Int;
    private var value:Any;
	private var readOnly:Boolean = true;
	private var deleted:Boolean = false;

    public def this(initVersion:Int, initValue:Any) {
    	this.initVersion = initVersion;
    	this.value = initValue;
    }
    
    public def update(n:Any) {
    	value = n;
    	readOnly = false;
    	if (deleted)
    		deleted = false;
    }
    
    public def delete() {
    	readOnly = false;
    	deleted = true;
    }
    
    public def getInitialVersion() = initVersion;
    public def getNewValue() = value;
    public def readOnly() = readOnly;
    public def isDeleted() = deleted;
}