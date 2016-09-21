package x10.util.resilient.localstore;

import x10.util.*;
import x10.util.concurrent.SimpleLatch;
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
    
    public var masterStore:MasterStore = null;
    private var slave:Place;
    
    public var slaveStore:SlaveStore = null;
    public var virtualPlaceId:Long = -1; //-1 means a spare place
    
    public def this(spare:Long) {
        val activePlaces = Place.numPlaces() - spare;
        if (here.id < activePlaces){
            virtualPlaceId = here.id;
        }
        val nextPlace = (here.id + 1) % Place.numPlaces();
        val prevPlace = (here.id - 1 + Place.numPlaces()) % Place.numPlaces();
        if (here.id < activePlaces){
            masterStore = new MasterStore(virtualPlaceId, nextPlace);
            slave = nextPlace;
        }
        if (here.id > 0 && here.id <= activePlaces ) {
            slaveStore = new SlaveStore(virtualPlaceId);
        }
    }
    
    
        
}