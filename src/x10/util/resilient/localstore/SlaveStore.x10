package x10.util.resilient.localstore;

import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;

public class SlaveStore {
    private val moduleName = "SlavePlace";
    private var epoch:Long;  //can be used for checking checkpointing consistency
    
    private val mastersMap:HashMap[Long,HashMap[String,Any]]; // master_virtual_id, master_data
    private transient val lock:Lock = new Lock();
    
    public def this() {
        mastersMap = new HashMap[Long,HashMap[String,Any]]();
    }
    
    //used to replace a previous slave
    public def addMasterPlace(newMasterVirtualId:Long, masterData:HashMap[String,Any], transLog:HashMap[String,TransKeyLog], masterEpoch:Long) {
        try {
            lock.lock();
            mastersMap.put(newMasterVirtualId, masterData);
            applyChangesLockAcquired(newMasterVirtualId, transLog, masterEpoch);
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
        var data:HashMap[Long,HashMap[String,Any]] = mastersMap.getOrElse(masterVirtualId, null);
        if (data == null) {
            data = new HashMap[Long,HashMap[String,Any]]();
            mastersMap.put(masterVirtualId, data);
        }
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
        epoch = masterEpoch;
    }
    
}