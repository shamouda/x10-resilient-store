package x10.util.resilient.localstore;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;

public class LocalTransaction (plh:PlaceLocalHandle[LocalDataStore], id:Long, placeIndex:Long) {
	private val moduleName = "LocalTransaction";
	
    private val transLog:HashMap[String,TransKeyLog] = new HashMap[String,TransKeyLog]();    
    
    public def put(key:String, newValue:Any):Any {
        var oldValue:Any = null;    
        val keyLog = transLog.getOrElse(key+placeIndex,null);
        if (keyLog != null) { // key used in the transaction before
            oldValue = keyLog.getValue();
            keyLog.update(newValue);
        }
        else {// first time to access this key
            val value = plh().masterStore.get(key);
            val log = new TransKeyLog(value);
            transLog.put(key, log);
            log.update(newValue);
            oldValue = value;
        }
        return oldValue;
    }
    
    
    public def delete(key:String):Any {
        var oldValue:Any = null;    
        val keyLog = transLog.getOrElse(key+placeIndex,null);
        if (keyLog != null) { // key used in the transaction before
            oldValue = keyLog.getValue();
            keyLog.delete();
        }
        else {// first time to access this key
            val value = plh().masterStore.get(key);
            val log = new TransKeyLog(value);
            transLog.put(key, log);
            log.delete();
            oldValue = value;
        }
        return oldValue;
    }
    
    
    public def get(key:String):Any {
        var oldValue:Any = null;
        val keyLog = transLog.getOrElse(key+placeIndex,null);
        if (keyLog != null) { // key used before in the transaction
           oldValue = keyLog.getValue();
        }
        else {
            val value = plh().masterStore.get(key);
            val log = new TransKeyLog(value);
            transLog.put(key, log);
            oldValue = value;
        }
        return oldValue;
    }

    //throws an exception if slave is dead
    public def commit() {
        try {
            plh().masterStore.epoch++;
            val masterEpoch = plh().masterStore.epoch;
            val masterVirtualId = plh().virtualPlaceId;
            at (plh().slave) {
                plh().slaveStore.applyMasterChanges(masterVirtualId, transLog, masterEpoch);
            }
        }
        catch(ex:Exception) {
            plh().masterStore.epoch--;
            throw ex;
        }
        
        //master commit
        plh().masterStore.applyChanges(transLog);
    }
    
}