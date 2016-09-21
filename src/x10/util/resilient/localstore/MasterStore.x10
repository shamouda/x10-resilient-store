package x10.util.resilient.localstore;

import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;

/*Assumption: no local conflicts*/
public class MasterStore {
    private val moduleName = "MasterStore";
    public var epoch:Long = 1;
    private val lock = new Lock();
    private val data:HashMap[String,Any] = new HashMap[String,Any]();
    private val virtualPlaceId:Long;
    
    public def this(virtualPlaceId:Long) {
        this.virtualPlaceId = virtualPlaceId;
    }
    
    public def get(key:String):Any {
        try {
            lock.lock();
            return data.getOrElse(key, null);
        }
        finally {
            lock.unlock();
        }
    }
    
    
    public def applyChanges(transLog:HashMap[String,TransKeyLog]) {
        try {
            lock.lock();
            val iter = transLog.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val log = transLog.getOrThrow(key);
                if (log.readOnly())
                    continue;
                if (log.isDeleted()) 
                    data.remove(key);
                else
                    data.update(key, transLog.getValue());
            }
        }
        finally {
            lock.unlock();
        }
    }
    
    public def getData() = data;
}
