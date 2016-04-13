package x10.util.resilient.map.transaction;

import x10.util.HashMap;
import x10.util.resilient.map.common.Utils;

public class TransKeyLog {
	private val initVersion:Int;
    private var value:Any;
    private val partitionId:Long;
	private var readOnly:Boolean = true;
	private var deleted:Boolean = false;

    public def this(initVersion:Int, initValue:Any, partitionId:Long) {
    	this.initVersion = initVersion;
    	this.value = initValue;
    	this.partitionId = partitionId;
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
    public def getValue() = value;
    public def readOnly() = readOnly;
    public def isDeleted() = deleted;
    public def getPartitionId() = partitionId;
    
    public def toString() = "InitVersion["+initVersion+"] value["+value
    		+"] partitionId["+partitionId+"] readOnly["+readOnly+"] deleted["+deleted+"]\n";
}