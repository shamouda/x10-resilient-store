package x10.util.resilient.map.transaction;

import x10.util.HashMap;
import x10.util.resilient.map.common.Utils;

public class TransKeyLog {
    /*The version of the value when the transaction started*/
    private val initVersion:Int;

    /*A copy of the value, used to isolate the transaction updates for the actual value*/
    private var value:Any;
    
    /*The partition that own the key/value record */
    private val partitionId:Long;
   
    /*A flag to indicate if the value was used for read only operations*/
    private var readOnly:Boolean = true;

    /*A flag to differentiate between setting a NULL and deleting an object*/
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
    
    public def toString(key:Any) = "Key["+key+"] InitVersion["+initVersion+"] value["+value
            +"] partitionId["+partitionId+"] readOnly["+readOnly+"] deleted["+deleted+"]\n";
}