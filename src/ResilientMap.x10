import x10.xrx.Runtime;
import x10.util.ArrayList;
import x10.compiler.NoInline;
//The interface for the application usage
//Object can only be created by the DataStore class
public class ResilientMap {
	private val name:String;
    private val strongConsistency:Boolean;

    def this(name:String, strong:Boolean) {
    	this.name = name;
    	this.strongConsistency = strong;
    }
    
    public def get(key:Any):Any {
    	val ds = DataStore.getInstance();
    	val partitionId = getPartitionId(key);
    	val mappingRecord = ds.getPartitionMapping(partitionId);
    	val primaryPlace = mappingRecord.getPrimaryPlace();   
    	var result:Any = null;
    	val mapName = name; // to avoid copying 'this' in the following at
    	
    	var retryCount:Long = 0;
    	while (retryCount < DataStore.DS_PARTITIONS_MAX_RETRY) {
    		try {
    			if (Place(primaryPlace).isDead()){
    				val secondaryPlace = mappingRecord.getSecondaryPlace();
    				if (Place(secondaryPlace).isDead()) {
    					continue; //retry
    				}
    		        result = at (Place(secondaryPlace)) {
    		        	val secContainer = DataStore.getInstance().getContainer();
	        	        secContainer.getSecondaryLocal(mapName, partitionId, key)
    		        };
    			}
    			else{
    				result = at (Place(primaryPlace)) {
    					val primContainer = DataStore.getInstance().getContainer();
	        	        primContainer.getPrimaryLocal(mapName, partitionId, key)
    				};
    			}
    		}
    		finally {
    			retryCount++;
    		}
    	}
    	if (retryCount == DataStore.DS_PARTITIONS_MAX_RETRY)
    		throw new Exception("Failed to get data from the key value store, reached maximum retry count ...");
    	return result;
    }
    
    public def put(key:Any, value:Any) {
    	Console.OUT.println(here + " - "+name+" - PUT ("+key+","+value+")    started ...");
    	val ds = DataStore.getInstance();
    	Console.OUT.println(here + " - "+name+" - PUT ("+key+","+value+")    got ds");
    	
    	val partitionId = getPartitionId(key);
    	Console.OUT.println(here + " - "+name+" - PUT ("+key+","+value+")    got partition id = " + partitionId);
    	
    	
    	val primaryIsAlive = ():Boolean => {
    		Console.OUT.println(here + " - "+name+" - PUT ("+key+","+value+")    checkprimary is alive");
    		val mappingRecord = ds.getPartitionMapping(partitionId);
    		Console.OUT.println(here + " - "+name+" - PUT ("+key+","+value+")    mappingRecord found  isNULL?"+(mappingRecord == null));
        	val primaryPlace = mappingRecord.getPrimaryPlace();
    		Console.OUT.println(here + " - "+name+" - PUT ("+key+","+value+")    primaryPlace is: "+primaryPlace);

        	return !Place(primaryPlace).isDead();
    	};
  
    	//If the primary place is dead, wait until another place becomes primary owner for this parition
    	//FIXME: might wait forever when both the primary and secondary die
    	sleepUntil(primaryIsAlive, here, "waiting until a primary place is available");
    	
    	/**** Put Status Values (negative means failed, positive means success) *****/
    	val STATUS_INIT = 0;
    	
    	//update invoked at put primary and secondary
        val STATUS_SUCCESS = 1;
        //eventualConsistency: only primary is updated, but the secondary is deed and might be repaired soon
        val STATUS_SUCCESS_SECONDARY_NEEDS_REPAIR = 2;
        
        //the partition has moved to another primary place
        val STATUS_CANCELLED_PARTITION_NOT_FOUND = -1;
        //strongConsistency: the primary place did not perform the put request because secondary is dead
        val STATUS_CANCELLED_SECONDARY_IS_DEAD = -2; 
        
        /*********Placeholders for the primary and secondary places to report their status************/
    	val primaryStatusGR = GlobalRef(new Cell[Long](STATUS_INIT));
        val secondaryStatusGR = GlobalRef(new Cell[Long](STATUS_INIT));
        //the actual secondary place will be reported by the primary place
        val secondaryPlaceGR = GlobalRef(new Cell[Long](-1)); 

    	var retryCount:Long = 0;
    	while (retryCount < DataStore.DS_PARTITIONS_MAX_RETRY) {
    		try{
    			val partMapping = ds.getPartitionMapping(partitionId);
    	        val primaryPlace = partMapping.getPrimaryPlace();   
    	        val mapName = name; // to avoid copying 'this' in the following at
    	        Console.OUT.println("From ["+here+"]  PUT_REQUEST  key["+key+"] value["+value+"]   PrimaryPlace["+primaryPlace+"] ...");
    	        
    	        primaryStatusGR()() = STATUS_INIT;
    	        secondaryStatusGR()() = STATUS_INIT;
    	        secondaryPlaceGR()() = -1;
    	        
    	        //update the primary place, the primary place will initiate put at the secondary place
	            at (Place(primaryPlace)) async {
	             	var putStatus:Long = STATUS_INIT;
	                var actualSecondaryPlaceId:Long = -1; 
	                //The primary will read the partition mapping again, because it can change with elasticity
	                val updatedPartitionMapping = ds.getPartitionMapping(partitionId);
	        	    if (updatedPartitionMapping.getPrimaryPlace() != here.id){
	        	    	/*this place is no longer the primary place, 
	        	    	 * the partition is stolen by a new joining place*/
	        	    	putStatus = STATUS_CANCELLED_PARTITION_NOT_FOUND;
	        	    }
	        	    else { //this place is the primary place
	        	    	val primContainer = DataStore.getInstance().getContainer();
	        	    	val secondaryPlace = updatedPartitionMapping.getSecondaryPlace();
	        	    	if (Place(secondaryPlace).isDead()) {
	        	    		if (strongConsistency) {
	        	    			putStatus = STATUS_CANCELLED_SECONDARY_IS_DEAD;
	        	    		}
	        	    		else {
	        	    			primContainer.putPrimaryLocal(mapName, partitionId, key, value);
    	        	    		putStatus = STATUS_SUCCESS_SECONDARY_NEEDS_REPAIR;
	        	    		}	
	        	    	}
	        	    	else {
	        	    		primContainer.putPrimaryLocal(mapName, partitionId, key, value);
	        	    		
	        	    		at (Place(secondaryPlace)) async {
	        	    			//FIXME: should I validate if this place still secondary????
	        	    			val secContainer = DataStore.getInstance().getContainer();
	    	        	        secContainer.putSecondaryLocal(mapName, partitionId, key, value);
	    	        	        at (secondaryStatusGR.home) async {
	    	        	    	    secondaryStatusGR()() = STATUS_SUCCESS;
	    	        	        }	
	        	    		}
	        	    		
	        	    		putStatus = STATUS_SUCCESS;
	        	    		actualSecondaryPlaceId = secondaryPlace;
	        	    	}
	        	    }
	        	    
	        	    val x = putStatus;
	        	    val y = actualSecondaryPlaceId;
	        	    at (primaryStatusGR.home) async {
        	    		primaryStatusGR()() = x;
        	    		secondaryPlaceGR()() = y;
        	    	}
	            }

    	        sleepUntil(() => primaryStatusGR()() != STATUS_INIT, Place(primaryPlace), "waiting for primary put");
    	        if (Place(primaryPlace).isDead() || primaryStatusGR()() < 0)
    	        	continue;
    	        
    	        if (strongConsistency) {
    	        	val secondaryPlace = Place(secondaryPlaceGR()());
    	            sleepUntil(() => secondaryStatusGR()() != STATUS_INIT, secondaryPlace, "waiting for secondary put");
    	            if (secondaryPlace.isDead() || secondaryStatusGR()() < 0)
    	            	continue;
    	        }
    	        
    	        //reaching here means put is successful
    	        break;
    		}finally {
    			retryCount++;
    		}
    	}
    	
    	if (retryCount == DataStore.DS_PARTITIONS_MAX_RETRY)
    		throw new Exception("Failed to put data in the key value store, reached maximum retry count ...");
    }
    
    val sleepUntil = (condition:() => Boolean, sourcePlace:Place, tag:String) => @NoInline {
    	var valid:Boolean = true;
        if (!condition()) {
            Runtime.increaseParallelism();
            while (!condition() && valid) {
                // look for dead source places
                if (sourcePlace.isDead()){
            	    valid = false;
                } 
                if (!valid)
                    System.threadSleep(0); // release the CPU to more productive pursuits
            }
            Runtime.decreaseParallelism(1n);
        }
    };
    
    
    private def getPartitionId(key:Any) : Long {
    	return key.hashCode() as Long % DataStore.DS_PARTITIONS_COUNT;
    }
    
    
}