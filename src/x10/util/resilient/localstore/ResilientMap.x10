package x10.util.resilient.localstore;

import x10.util.Team;
import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;
import x10.util.resilient.iterative.PlaceGroupBuilder;

public class ResilientMap {
    private val moduleName = "LocalStoreMap";
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
    
    public def recoverDeadPlaces(deadPlaces:ArrayList[Place]) {
        
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