import x10.util.concurrent.SimpleLatch;
import x10.util.ArrayList;
import x10.util.RailUtils;
import x10.util.HashMap;
import x10.util.HashSet;

//Concurrency:  multiple threads
public class Replica {
	private val replicaId = here.id;
	
    private val paritions:HashMap[Long,Partition] = new HashMap[Long,Partition]();    
	private val partitionsLock:SimpleLatch;
	
	
	private val transactions:HashMap[Long,TransLog] = new HashMap[Long,TransLog]();
	private val transactionsLock:SimpleLatch;

    public def this(partitionIds:HashSet[Long]) {	       
    	createPartitions(partitionIds);
    	partitionsLock = new SimpleLatch();
    	transactionsLock = new SimpleLatch();
    }
    
    private def createPartitions(partitionIds:HashSet[Long]) {
    	val iter = partitionIds.iterator();
    	while (iter.hasNext()) {
    		val key = iter.next();
    		paritions.put(key,new Partition(key));
    	}
    }
    
   
   /*
    public def putPrimaryLocal(mapName:String, partitionId:Long, key:Any, value:Any):Boolean {
    	val partition = primaryPartitions.getOrElse(partitionId, null);
    	if (partition == null)
    		return false;
    	
    	partition.put(mapName, key, value);
    	return true;
    }  
    
    public def getPrimaryLocal(mapName:String, partitionId:Long, key:Any):Any {
    	val partition = primaryPartitions.getOrElse(partitionId, null);
    	if (partition == null)
    		return null;
    	
    	return partition.get(mapName, key);
    }
    */
    
    public def toString():String {
    	var str:String = "Primary [";
    	
        str += "]\n";
        return str;
    }
}