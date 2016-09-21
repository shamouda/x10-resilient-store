package x10.util.resilient.localstore;

import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;

public class MasterStore {
    private val moduleName = "MasterStore";

    private val data:HashMap[String,Any] = new HashMap[String,Any]();
    
    private val masterLock = new Lock();
    private val keyLocks = new HashMap[String,Lock]();
    private val transLogs = new HashMap[Long,TransLog]();
    
    private val virtualPlaceId:Long;
    
    public def this(virtualPlaceId:Long) {
        this.virtualPlaceId = virtualPlaceId;
    }
    
    public def put(txId:Long, key:String, value:Any):Any {
        val params = getRequestParams(txId, key);
        var logOldValue:Boolean = false;
        if (!params.transLog.contains(key)) {
            params.keyLock.lock();
            params.transLog.addKeyLock(key, params.keyLock);
            logOldValue = true;
        }
        val oldValue = data.put(key, value);
        if (logOldValue)
            params.transLog.setOldValue(key, oldValue);
        return oldValue;
    }
    
    public def get(txId:Long, key:String):Any {
        val params = getRequestParams(txId, key);
        if (!params.transLog.contains(key)) {
            params.keyLock.lock();
            params.transLog.addKeyLock(key, params.keyLock);
        }
        
        val value = data.get(key);
        return value;
    }
    

    public def commit(txId:Long) {
        commitSlave();
        commitMaster();
    }
    
    private def commitMaster() {
        val ulog = getTransLog(txId);
        val keyLocks = ulog.getKeyLocks();
        
        //unlock all locked keys
        val keyIter = keyLocks.keySet().iterator();
        while(keyIter.hasNext()) {
            val key = keyIter.next();
            val lock = keyLocks.getOrThrow(key);
            lock.unlock();
        }
        removeTransLog(txId);
    }
    

    
    public def rollback(txId:Long) {
        val ulog = getTransLog(txId);
        val keyLocks = ulog.getKeyLocks();
        val oldValues = ulog.getOldValues();
        
        //return to old values
        val iter = oldValues.keySet().iterator();
        while(iter.hasNext()) {
            val key = iter.next();
            val oldValue = oldValues.getOrThrow(key);
            data.put(key, oldValue);
        }
        
        //unlock all locked keys
        val keyIter = keyLocks.keySet().iterator();
        while(keyIter.hasNext()) {
            val key = keyIter.next();
            val lock = keyLocks.getOrThrow(key);
            lock.unlock();
        }
        removeTransLog(txId);
    }
    
    
    private def getRequestParams(txId:Long, key:String):RequestParams {
        try {
            masterLock.lock(); // lock used to avoid creating some two key locks for 2 concurrent requests.
            val lock = keyLocks.getOrElse(key, new Lock());
            val ulog = transLogs.getOrElse(txId,new TransLog());
            transLogs.put(txId,ulog);
            return new RequestParams(lock, ulog);
        }finally {
            masterLock.unlock();
        }
    }
    private def getTransLog(txId:Long):TransLog {
        try {
            masterLock.lock(); // lock used to avoid creating some two key locks for 2 concurrent requests.
            val ulog = transLogs.getOrElse(txId,new TransLog());
            transLogs.put(txId,ulog);
            return ulog;
        }finally {
            masterLock.unlock();
        }
    }

    private def removeTransLog(txId:Long):void {
        try {
            masterLock.lock();
            transLogs.remove(txId);
        }finally {
            masterLock.unlock();
        }
    }
        
}

class RequestParams (keyLock:Lock, transLog:TransLog) {     }
