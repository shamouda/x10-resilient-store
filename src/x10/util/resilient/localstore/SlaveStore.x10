package x10.util.resilient.localstore;

import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;

public class SlaveStore {
    private val moduleName = "SlavePlace";
    
    private val mastersMap:HashMap[Long,HashMap[String,Any]]; // master_virtual_id, master_data
    private transient val lock:SimpleLatch = new SimpleLatch;
    
    /*Must be called by the master place before issuing any commits*/
    public def addMasterPlace(masterVirtualId:Long) {
        try {
            lock.lock();
            val dataMap = new HashMap[String,Any]();
            mastersMap.put(masterVirtualId, dataMap);            
        }
        finally {
            lock.release();
        }
    }
    
    /*The master is sure about commiting these changes, go ahead and apply them*/
    public def applyChanges(masterVirtualId:Long, data:HashMap[String,Any]) {
        try {
            lock.lock();
            val map = mastersMap.getOrThrow(masterVirtualId);
            val iter = data.KeySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val value = data.getOrThrow(key);
                map.put(key,value);
            }
        }
        finally {
            lock.release();
        }
    }
    
}