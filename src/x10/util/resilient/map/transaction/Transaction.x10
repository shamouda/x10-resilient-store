package x10.util.resilient.map.transaction;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.resilient.map.common.Utils;

/*
 * Used to log the changes in key values by a single transaction
 * The object is expected to be used by only one thread
 * */
public class Transaction {
    private val moduleName = "Transaction";
    public static val VERBOSE = Utils.getEnvLong("TRANS_LOG_VERBOSE", 0) == 1 || Utils.getEnvLong("DS_ALL_VERBOSE", 0) == 1;
    
    /*Transaction Id*/
    public val transId:Long;
    
    /*The used keys by a the transaction, and their logs*/
    private val cache:HashMap[Any,TransKeyLog] = new HashMap[Any,TransKeyLog]();

    /*Transaction start time*/
    private val startTimeMillis:Long;
    
    /* The client which issued the transaction. If the client dies, the Replica will abort all its transactions*/
    public val clientPlaceId:Long;
    
    public def this(transId:Long, startTimeMillis:Long, clientPlaceId:Long){
        this.transId = transId;
        this.startTimeMillis = startTimeMillis;
        this.clientPlaceId = clientPlaceId;
    }
    
    /*Logs a 'get' operation on a key. If the key was not used before by the transaction, a new log record is created for that key*/
    public def logGet (key:Any, initVersion:Int, initValue:Any, partitionId:Long) {
        var cacheRec:TransKeyLog = cache.getOrElse(key,null);
        if (cacheRec == null) {
            cacheRec = new TransKeyLog(initVersion, initValue, partitionId);
            cache.put(key, cacheRec);
        }
        cache.put(key,cacheRec);
    }
    
    /*Logs an 'update' operation on a key. If the key was not used before by the transaction, a new log record is created for that key*/
    public def logUpdate(key:Any, initVersion:Int, initValue:Any, newValue:Any, partitionId:Long) {
        var cacheRec:TransKeyLog = cache.getOrElse(key,null);
        if (cacheRec == null) {
            cacheRec = new TransKeyLog(initVersion, initValue, partitionId);
            cache.put(key, cacheRec);
        }
        cacheRec.update(newValue);
    }
    
    /*Logs an 'update' operation on a key that was used before by the transaction*/
    public def logUpdate(key:Any, newValue:Any) {
        val cacheRec = cache.getOrThrow(key);
        cacheRec.update(newValue);
    }
    
    /*Logs a 'delete' operation on a key. If the key was not used before by the transaction, a new log record is created for that key*/
    public def logDelete(key:Any, initVersion:Int, initValue:Any, partitionId:Long) {
        var cacheRec:TransKeyLog = cache.getOrElse(key,null);
        if (cacheRec == null) {
            cacheRec = new TransKeyLog(initVersion, initValue, partitionId);
            cache.put(key, cacheRec);
        }
        cacheRec.delete();
    }
   
    /*Logs a 'delete' operation on a key that was used before by the transaction*/
    public def logDelete(key:Any) {
        val cacheRec = cache.getOrThrow(key);
        cacheRec.delete();
    }
    
    /*Checks if two transactions are conflicting*/
    public def isConflicting (other:Transaction):Boolean {
        var result:Boolean = false;
        val overlap = getOverlappingKeys(other);
        for (key in overlap){
            if (!cache.getOrThrow(key).readOnly() || !other.cache.getOrThrow(key).readOnly()){
                if (VERBOSE) Utils.console(moduleName, "Tx("+transId+") and Tx("+(other.transId)+") conflict in key ["+key+"]");
                result = true;
                break;
            }
        }
        if (VERBOSE) Utils.console(moduleName, "Tx("+transId+") and Tx("+(other.transId)+") conflicting=["+result+"] ...");
        return result;
    }
    
    /*Returns a list of keys used by the two transactions*/
    private def getOverlappingKeys(other:Transaction):ArrayList[Any] {
        val iter = cache.keySet().iterator();
        val list = new ArrayList[Any]();
        while (iter.hasNext()){
            val key = iter.next();
            try{
                other.cache.getOrThrow(key);
                list.add(key);
            }catch(ex:Exception){}
        }
        
        if (VERBOSE){
            if (list.size() == 0)
                Utils.console(moduleName,"Tx("+transId+") and Tx("+(other.transId)+") no overlap");
            else{
                var str:String = "";
                for (x in list)
                    str += x + ",";
                Utils.console(moduleName,"Tx("+transId+") and Tx("+(other.transId)+") overlapped keys = " + str);
            }
        }
        return list;
    }
    
    public def getKeysCache() = cache;
    
    
    public def isPartitionUsedForUpdate(partitionId:Long) {
        val iter = cache.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val keyLog = cache.getOrThrow(key);
            if (keyLog.getPartitionId() == partitionId && !keyLog.readOnly()) {
                return true;
            }
        }
        return false;
    }

}