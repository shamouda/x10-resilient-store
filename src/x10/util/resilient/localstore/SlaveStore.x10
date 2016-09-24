package x10.util.resilient.localstore;

import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;
import x10.util.concurrent.Lock;

public class SlaveStore {
    private val moduleName = "SlaveStore";
    
    private val mastersMap:HashMap[Long,MasterState]; // master_virtual_id, master_data
    private transient val lock:Lock = new Lock();
    
    public def this() {
        mastersMap = new HashMap[Long,HashMap[String,Any]]();
    }
    
    public def addMasterPlace(masterVirtualId:Long, masterData:HashMap[String,Any], transLog:HashMap[String,TransKeyLog], masterEpoch:Long) {
        try {
            lock.lock();
            mastersMap.put(masterVirtualId, new MasterState(masterData,masterEpoch));
            applyChangesLockAcquired(masterVirtualId, transLog, masterEpoch);
        }
        finally {
            lock.unlock();
        }
    }
    
    
    public def getMasterState(masterVirtualId:Long):MasterState {
        try {
            lock.lock();
            return mastersMap.getOrThrow(masterVirtualId);
        }
        finally {
            lock.unlock();
        }
    }
    public def applyMasterChanges(masterVirtualId:Long, transLog:HashMap[String,TransKeyLog], masterEpoch:Long) {
        try {
            lock.lock();
            applyChangesLockAcquired(newMasterVirtualId, transLog, masterEpoch);
        }
        finally {
            lock.unlock();
        }
    }
    
    /*The master is sure about commiting these changes, go ahead and apply them*/
    private def applyChangesLockAcquired(masterVirtualId:Long, transLog:HashMap[String,TransKeyLog], masterEpoch:Long) {
        var state:MasterState = mastersMap.getOrElse(masterVirtualId, null);
        if (state == null) {
            state = new MasterState(new HashMap[String,Any](), masterEpoch);
            mastersMap.put(masterVirtualId, state);
        }
        val data = state.data;
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
        state.epoch = masterEpoch;
    }
}

class MasterState {
    public var epoch:Long;
    public var data:HashMap[String,Any];
    public def this(data:HashMap[String,Any], epoch:Long) {
        this.data = data;
        this.epoch = epoch;
    }
}