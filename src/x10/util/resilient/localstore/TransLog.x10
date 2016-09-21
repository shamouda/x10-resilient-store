package x10.util.resilient.localstore;

import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;
import x10.util.concurrent.Lock;

public class TransLog {
    private oldValues:HashMap[String,Any] = new HashMap[String,Any]();
//TODO: what if the old value is null
    private keyLocks:HashMap[String,Lock] = new HashMap[String,Lock]();

    public def setOldValue(key:String, oldValue:Any) { // for put
        oldValues.put(key, oldValue);
    }
    
    public def addKeyLock(key:String, lock:Lock) { // for get
        keyLocks.put(key, lock);
    }
    
    public def getOldValue(key:String) {
        return oldValues.getOrElse(key,null);
    }   
    
    public def contains(key:String) {
        return keyLocks.keySet().contains(key);
    }
    
    public def getOldValues() = oldValues;
    public def getKeyLocks() = keyLocks;
}