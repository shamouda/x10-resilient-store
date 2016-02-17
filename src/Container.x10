import x10.util.concurrent.SimpleLatch;
import x10.util.ArrayList;
import x10.util.RailUtils;
import x10.util.HashMap;

//each place has 1 container that actually holds the partitions and the partition table
public class Container {
    private val primaryPartitions:HashMap[Long,Partition];
    private val secondaryPartitions:HashMap[Long,Partition];
    
    public def this(segments:Rail[Int]) {
	    val offsets = RailUtils.scanExclusive(segments, (x:Int, y:Int) => x+y, 0n);   
	    primaryPartitions = createPrimaryPartitionsList(segments, offsets);
	    secondaryPartitions = createSecondaryPartitionsList(segments, offsets);
    }
   
    public def isPrimary(partitionId:Long):Boolean {
    	val partition = primaryPartitions.getOrElse(partitionId, null);
    	if (partition == null)
    		return false;
    	return true;
    } 
    
    public def putPrimaryLocal(mapName:String, partitionId:Long, key:Any, value:Any):Boolean {
    	val partition = primaryPartitions.getOrElse(partitionId, null);
    	if (partition == null)
    		return false;
    	
    	partition.put(mapName, key, value);
    	return true;
    }
    
    public def putSecondaryLocal(mapName:String, partitionId:Long, key:Any, value:Any):Boolean {
    	val partition = secondaryPartitions.getOrElse(partitionId, null);
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
    
    public def getSecondaryLocal(mapName:String, partitionId:Long, key:Any):Any {
    	val partition = secondaryPartitions.getOrElse(partitionId, null);
    	if (partition == null)
    		return null;
    	return partition.get(mapName, key);
    }
    
    
    static def createPrimaryPartitionsList(segments:Rail[Int], offsets:Rail[Int]) {
    	val myIndex = here.id-DataStore.DS_FIRST_PLACE;
    	val primaryPartitions = new HashMap[Long,Partition]();
    	val primStart = offsets(myIndex);
    	val primCnt = segments(myIndex);
    	for (var i:Long = 0; i < primCnt ; i++){
    		primaryPartitions.put(i+primStart, new Partition(i+primStart,true));
    	}
    	return primaryPartitions;
    }
    
    static def createSecondaryPartitionsList(segments:Rail[Int], offsets:Rail[Int]) {
    	val myIndex = here.id-DataStore.DS_FIRST_PLACE;
    	val secondaryPartitions = new HashMap[Long,Partition]();
    	val secIndex = (myIndex+1)%DataStore.DS_INITIAL_MEMBERS_COUNT;
    	val secStart = offsets(secIndex);
    	val secCnt = segments(secIndex);
    	for (var i:Long = 0; i < secCnt ; i++){
    		secondaryPartitions.put(i+secStart, new Partition(i+secStart,false));
    	}	
    	return secondaryPartitions;
    }
    
    public def toString():String {
    	var str:String = "Primary [";
    	if (primaryPartitions != null) {
    		val iter = primaryPartitions.keySet().iterator();
    		while (iter.hasNext()){
    			str += iter.next() + ",";
    		}
    	}
        str += "]\nSecondary [";
        if (secondaryPartitions != null) {
        	val iter = secondaryPartitions.keySet().iterator();
    		while (iter.hasNext()){
    			str += iter.next() + ",";
    		}
        }
        str += "]\n";
        return str;
    }
}