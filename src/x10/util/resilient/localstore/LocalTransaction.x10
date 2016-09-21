package x10.util.resilient.localstore;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;
import x10.util.resilient.map.transaction.
public class LocalTransaction (plh:PlaceLocalHandle[LocalDataStore], id:Long) {
    private val transLog:HashMap[String,TransKeyLog] = new HashMap[String,TransKeyLog]();    
    
    public def put(key:String, newValue:Any):Any {
        var oldValue:Any = null;    
        val keyLog = transLog.getOrElse(key,null);
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
        val keyLog = transLog.getOrElse(key,null);
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
        val keyLog = transLog.getOrElse(key,null);
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
 
    public def rollback() {
        //ignore transaction log
        transLog.clear();
    }

    public def commit() {
        try {
            plh().masterStore.epoch++;
            commitSlave();
        }
        catch(ex:Exception) {
            plh().masterStore.epoch--;
            throw ex;
        }
        
        //master commit
        plh().masterStore.applyChanges(transLog);
    }
    
    private def commitSlave() {
        val masterVirtualId = plh().virtualPlaceId;
        val masterEpoch = plh().masterStore.epoch;
        var retryCount:Long = 0;
        val retryMax:Long = 5;
        while (retryCount < retryMax) {
            try {
                val initSlave = handleDeadSlave();
                val slave = plh().slave;
                if (initSlave) {
                    val masterData = plh().masterStore.getData();
                    at (slave) {
                        plh().updateSlave(masterVirtualId, masterData, transLog, masterEpoch);
                    }
                }
                else {
                    at (slave) {
                        plh().applyMasterChanges(masterVirtualId, transLog, masterEpoch);
                    }
                }
                break;
            }
            catch(ex:Exception) {
                 
            }
            retryCount++;
        }
        
        if (retryCount == retryMax) {
            throw new Exception("Failed to commit slave");
        }
    }
    private def handleDeadSlave():Boolean {
        var initSlave:Boolean = false;
        if (plh().slave.isDead()) {
            var newSlave:Place = null;
            val deadSlaveId = slave.id;
            for (var i=1; i<Place.numPlaces(); i++) {
                if (!Place((deadSlaveId+i)%Place.numPlaces()).isDead()){
                    newSlave = Place((deadSlaveId+i)%Place.numPlaces());
                    break;
                }
            }
            plh().slave = newSlave;
            initSlave = true;
        }
        return initSlave;
    }
   
    /*Logs a 'delete' operation on a key that was used before by the transaction*/
    public def logDelete(key:String) {
        val log = transLog.getOrThrow(key);
        log.delete();
    }
    
}