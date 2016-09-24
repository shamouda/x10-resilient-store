package x10.util.resilient.localstore;

import x10.util.Team;
import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;
import x10.util.resilient.iterative.PlaceGroupBuilder;

public class ResilientMap {
    private val moduleName = "ResilientMap";
    private val plh:PlaceLocalHandle[LocalDataStore];
    private var activePlaces:PlaceGroup;
    private var sparePlaces:ArrayList[Place];
    private var deadPlaces:ArrayList[Place];
    private val slaveMap:Rail[Long]; //master virtual place to slave physical place
    
    public def this(spare:Long){
        activePlaces = PlaceGroupBuilder.execludeSparePlaces(spare);
        slaveMap = new Rail[Long](activePlaces.size, (i:long) => { (i + 1) % activePlaces.size} );
        plh = PlaceLocalHandle.make[LocalDataStore](Place.places(), () => new LocalDataStore(spare, slaveMap));
        
        sparePlaces = new ArrayList[Place]();
        for (var i:Long = activePlaces.size(); i< Place.numPlaces(); i++){
            sparePlaces.add(Place(i));
        }
    }
    
    public def getVirtualPlaceId() = activePlaces.indexOf(here);
    
    public def getActivePlaces() = activePlaces;
    
    public def recoverDeadPlaces() {
        val oldPlaceGroup = activePlaces;
        val addedSparePlaces = new HashMap[Long,Long](); // key=readId, value=virtualPlaceId
        val mastersLostTheirSlaves = new ArrayList[Long]();
        val group = new x10.util.ArrayList[Place]();
        var deadCount:Long = 0;
        var allocated:Long = 0;
        var virtualPlaceId:Long = 0;
        for (p in oldPlaceGroup){
            if (p.isDead()){
                deadCount++;
                if (sparePlaces.size() > 0){
                    val sparePlace = sparePlaces.removeAt(0);
                    Console.OUT.println("place ["+sparePlace.id+"] is replacing ["+p.id+"] since it is dead ");
                    group.add(sparePlace);
                    addedSparePlaces.put(sparePlace.id,virtualPlaceId);
                    allocated++;
                }
                else
                    throw new Exception("No enough spare places found ...");
                
                //FIXME: may be more than one
                mastersLostTheirSlaves.add(findMasterVirtualIdGivenSlave(p.id));
            }
            else{
                Console.OUT.println("adding place p["+p.id+"]");
                group.add(p);
            }
            virtualPlaceId++;
        }
        
        activePlaces = new SparsePlaceGroup(group.toRail());
        
        checkIfBothMasterAndSlaveLost(addedSparePlaces, mastersLostTheirSlaves);
        
        recoverMasters(addedSparePlaces);
        
        recoverSlaves(mastersLostTheirSlaves);
        
    }
    
    private def checkIfBothMasterAndSlaveLost(addedSparePlaces:HashMap[Long,Long], mastersLostTheirSlaves:ArrayList[Long]) {
        val iter = addedSparePlaces.keySet().iterator();
        if (iter.hasNext()) {
            val masterRealId = iter.next();
            val masterVirtualId = addedSparePlaces.getOrThrow(masterRealId);
            if (mastersLostTheirSlaves.contains(masterVirtualId)) {
                throw new Exception("Fatal: both master and slave lost for virtual place["+masterVirtualId+"] ");
            }
        }
    }
    
    private def findMasterVirtualIdGivenSlave(slaveRealId:Long) {
        for (var i:Long = 0; i < activePlaces.size(); i++) {
            if (slaveMap(i) == slaveRealId)
                return i;
        }
        throw new Exception("Fatal error: could not find master for slave at ["+slaveRealId+"]");
    }
    
    private def recoverMasters(addedSparePlaces:HashMap[Long,Long]) {
        val iter = addedSparePlaces.keySet().iterator();
        finish {
            while (iter.hasNext()) {
                val realId = iter.next();
                val virtualId = addedSparePlaces.getOrThrow(realId);
                val slaveRealId = slaveMap(virtualId);
                val slave = Place(slaveRealId);
                if (slave.isDead())
                    throw new Exception("[Fatal] Both Master and Slave are Dead");
                
                at (slave) async {
                    val masterState = plh().slaveStore.getMasterState(virtualId);
                    at (Place(realId)) {
                        plh().joinAsMaster (virtualId, masterState.data, masterState.epoch);
                    }
                }
            }
        }
    }
    
    private def recoverSlaves(mastersLostTheirSlaves:ArrayList[Long]) {
        val masterNewSlave = new HashMap[Long,Long](); // key=masterVirtualId, value=slaveRealId
        for (masterVirtualId in mastersLostTheirSlaves) {
            val oldSlaveRealId = Place(slaveMap(masterVirtualId)).id;
            var found:Boolean = false;
            var newSlaveRealId:Long = 0;
            for (var i:Long = 1; i < Place.numPlaces(); i++){
                if (!Place(oldSlaveRealId + i).isDead()) {
                    found = true;
                    newSlaveRealId = Place(oldSlaveRealId + i).id;
                    masterNewSlave.put(masterVirtualId, newSlaveRealId);
                    break;
                }
            }
            if (!found)
                throw new Exception("[Fatal] could not find a new slave");
        }
        
        
        finish {
            val iter = masterNewSlave.keySet().iterator();
            while (iter.hasNext()) {
                val masterVirtualId = iter.next();
                val slaveRealId:Long = masterNewSlave.getOrThrow(masterVirtualId);
                at (activePlaces(masterVirtualId)) {
                    val masterState = plh().masterStore.getState(); 
                    at (Place(slaveRealId)) {
                        plh().slaveStore.addMasterPlace(masterVirtualId, masterState.data, new HashMap[String,TransKeyLog](), masterState.epoch);
                    }
                }
            }
        }
        
        val iter = masterNewSlave.keySet().iterator();
        while (iter.hasNext()) {
            val masterVirtualId = iter.next();
            val slaveRealId:Long = masterNewSlave.getOrThrow(masterVirtualId);
            slaveMap(masterVirtualId) = slaveRealId;
        }
    }
    
    public def startLocalTransaction():LocalTransaction {
        assert(plh().virtualPlaceId != -1);
        return new LocalTransaction(plh, Utils.getNextTransactionId());
    }
    
    public def startGlobalTransaction(places:PlaceGroup):GlobalTransaction {
        assert(plh().virtualPlaceId != -1);
        return new GlobalTransaction(plh, Utils.getNextTransactionId(), places);
    }
    
    
}