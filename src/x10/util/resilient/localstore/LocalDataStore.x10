package x10.util.resilient.localstore;

import x10.util.*;
import x10.util.concurrent.Lock;
import x10.compiler.Inline;
import x10.xrx.Runtime;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.impl.ResilientMapImpl;
import x10.util.resilient.map.partition.PartitionTable;
import x10.util.resilient.map.common.Topology;
import x10.util.resilient.map.impl.Replica;
import x10.util.resilient.map.impl.ReplicaClient;
import x10.util.resilient.map.exception.TopologyCreationFailedException;
import x10.util.resilient.map.exception.InvalidDataStoreException;
import x10.util.resilient.map.migration.MigrationHandler;
import x10.util.resilient.map.transaction.TransactionRecoveryManager;
import x10.compiler.Ifdef;

public class LocalDataStore {
    private val moduleName = "LocalDataStore";
    
    private val lock = new Lock();
    public var masterStore:MasterStore = null;
    private var slave:Place;
    
    public var slaveStore:SlaveStore = null;
    public var virtualPlaceId:Long = -1; //-1 means a spare place
    
    public def this(spare:Long, slaveMap:Rail[Long]) {
        val activePlaces = Place.numPlaces() - spare;
        if (here.id < activePlaces){
            virtualPlaceId = here.id;
            slave = Place(slaveMap(virtualPlaceId));
            masterStore = new MasterStore(virtualPlaceId);
            slaveStore = new SlaveStore();
        }
        
    }
    
    public def updateSlave(masterVirtualId:Long, masterData:HashMap[String,Any], transLog:HashMap[String,TransKeyLog]) {
        try{
            lock.lock();
            if (slaveStore==null)
                slaveStore = new SlaveStore(masterVirtualId, masterData, transLog);
            else
                slaveStore.addMasterPlace(masterVirtualId, masterData, transLog);
            
        }finally {
            lock.unlock();
        }
    }
    
        
}