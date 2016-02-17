import x10.util.HashMap;
import x10.util.concurrent.SimpleLatch;

public class PartitionTable {
	//partition_Id,PartitionRecord {primaryPlace,secondaryPlace}
	private var partMap:HashMap[Long,PartitionRecord];
	
	//place_Id,Num_of_owned_partitions,  TODO: we might not need this
	private var placeMap:HashMap[Long,Long];
	
	private val lock:SimpleLatch;
	
    public def this(segments:Rail[Int]) {
    	partMap = createParitionMap(segments); // partition-place mapping
    	placeMap = createPlaceCountMap(segments); //place-countOfPartitions mapping
    	lock = new SimpleLatch();
    }
    
    public def toString():String {
        var str:String = "===Partition Table====\n partId -> primary -> secondary\n";
        val iter = partMap.keySet().iterator();
        while (iter.hasNext()){
        	val partId = iter.next();
        	val record = partMap.get(partId);
        	val prim = record.getPrimaryPlace();
        	val sec = record.getSecondaryPlace();
        	
        	str += partId + " -> " + prim + " -> " + sec + "\n";
        }
        
        str += "===Places Counts====\n placeId => count\n";
        val iter2 = placeMap.keySet().iterator();
        while (iter2.hasNext()){
        	val placeId = iter2.next();
        	val count = placeMap.get(placeId);
        	str += placeId + " => " + count + "\n";
        }        
        return str;
    }
    
    public def get(partitionId:Long):PartitionRecord {
        try{
    	    lock.lock();
    	    return partMap.getOrElse(partitionId,null);
        }finally{
    	    lock.unlock();
        }
    }
    
    private static def createParitionMap(segments:Rail[Int]):HashMap[Long,PartitionRecord] {
    	Console.OUT.println("Creating Paritions Map ...");
    	val partMap = new HashMap[Long,PartitionRecord]();
    	val lastPlace = DataStore.DS_FIRST_PLACE+DataStore.DS_INITIAL_MEMBERS_COUNT-1;
    	
    	var idx:Long = 0;
    	var cnt:Int = 0N;
    	var prim:Long = DataStore.DS_FIRST_PLACE;
        var sec:Long = prim+1;
    	if (sec > lastPlace)
    		sec = DataStore.DS_FIRST_PLACE;
    	
    	for (var i:Long = 0; i < DataStore.DS_PARTITIONS_COUNT; i++){	
    	    //Console.OUT.println("cmp " + cnt + "," + segments(idx));
    		if (cnt == segments(idx)){
    			idx++;
    			prim++;
    			sec = prim+1;
    			if (sec > lastPlace)
    	    		sec = DataStore.DS_FIRST_PLACE;
    			cnt = 0N;
    		}
    		val record = new PartitionRecord(prim, sec);
    		partMap.put(i,record);
    		cnt++;
    	}
    	Console.OUT.println("Creating Paritions Map Succeeded ...");
        return partMap;
    }
    
    private static def createPlaceCountMap(segments:Rail[Int]):HashMap[Long,Long] {
    	val placeMap = new HashMap[Long,Long]();
    	for (var i:Long = 0; 
    	     i<DataStore.DS_INITIAL_MEMBERS_COUNT; 
    	     i++){
    		placeMap.put(i+DataStore.DS_FIRST_PLACE, segments(i));
    	}
    	return placeMap;
    }
    
    //Periodically called by the leader 
    public def updatePartitionTable(partMap:HashMap[Long,PartitionRecord], placeMap:HashMap[Long,Long]) {
    	lock.lock();
    	this.partMap = partMap;
        this.placeMap = placeMap;
    	lock.unlock();
    }
    
    public def addOrUpdatePartitionMapping (partitionId:Long, primaryPlace:Long, secondaryPlace:Long) {
    	lock.lock();
    	//Update the partition-place mapping
    	var record:PartitionRecord = partMap.getOrElse(partitionId,null);
    	if (record == null){
    		record = new PartitionRecord();
    	}
    	var oldPrimaryPlace:Long = record.getPrimaryPlace();
        record.update(primaryPlace,secondaryPlace);
    	partMap.put(partitionId,record);
    	
    	//Update the place-partitionCount mapping
    	if (oldPrimaryPlace != primaryPlace){
    		if (oldPrimaryPlace == -1){
    		    var count:Long = placeMap.getOrElse(primaryPlace,0) + 1;
    		    placeMap.put(primaryPlace,count);
    		}
    		else{
    			var countOld:Long = placeMap.getOrElse(oldPrimaryPlace,0);
    		    placeMap.put(oldPrimaryPlace,--countOld);
    		    
    			var count:Long = placeMap.getOrElse(primaryPlace,0) + 1;
		        placeMap.put(primaryPlace,count);
    		}
    	}
    	lock.unlock();
    }
    
}

class PartitionRecord {
	private var primaryPlace:Long;
    private var secondaryPlace:Long;

    public def this() {
    	primaryPlace = -1;
    	secondaryPlace = -1;
    }
	
    public def this(primary:Long, secondary:Long) {
    	primaryPlace = primary;
    	secondaryPlace = secondary;
    }
    public def getPrimaryPlace():Long = primaryPlace;
    public def getSecondaryPlace():Long = secondaryPlace;
    public def setPrimaryPlace(p:Long) {
    	primaryPlace = p;
    }
    public def setSecondaryPlace(p:Long) {
    	secondaryPlace = p;
    }
    public def update(primary:Long, secondary:Long) {
    	primaryPlace = primary;
    	secondaryPlace = secondary;
    }
}