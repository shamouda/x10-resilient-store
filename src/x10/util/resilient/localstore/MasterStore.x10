package x10.util.resilient.localstore;

import x10.util.HashSet;
import x10.util.HashMap;
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
    private val data:HashMap[String,Any];
    private val virtualPlaceId:Long;
    
    //used for original active places joined before any failured
    public def this(virtualPlaceId:Long) {
        this.virtualPlaceId = virtualPlaceId;
        this.data = new HashMap[String,Any]();
    }
    
    //used when a spare place is replacing a dead one
    public def this(virtualPlaceId:Long, data:HashMap[String,Any], epoch:Long) {
        this.virtualPlaceId = virtualPlaceId;
        this.data = data;
        this.epoch = epoch;
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
                    data.put(key, log.getValue());
            }
        }
        finally {
            lock.unlock();
        }
    }
    
    public def getState():MasterState {
        try {
            lock.lock();
            return new MasterState(data, epoch);
        }
        finally {
            lock.unlock();
        }
    }
}
