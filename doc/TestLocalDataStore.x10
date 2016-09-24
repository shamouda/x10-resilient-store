import harness.x10Test;

import x10.util.resilient.map.*;
import x10.util.Random;
import x10.util.ArrayList;
import x10.util.Timer;
import x10.util.Team;
import x10.util.concurrent.AtomicBoolean;
import x10.util.HashMap;

import x10.util.Option;
import x10.util.OptionsParser;
import x10.util.resilient.iterative.*;

public class TestLocalDataStore(iterations:Long) implements LocalDataStoreIterativeApp {
    var plh:PlaceLocalHandle[LocalDomain];
    var team:Team;
    val size:Long = 10;
    
    public def init(places:PlaceGroup, team:Team) {
        plh = PlaceLocalHandle.make[Domain](places, () => new LocalDomain(size));
        this.team = team;
    }
    
    public def isFinished():Boolean {
        plh().curIter == iterations;
    }
    
    public def step() {
        plh().add(here.id);
        team.allreduce(plh().array, 0, plh().array, 0, size, Team.ADD);
    }
    
    public def getCheckpointAndRestoreKeys():Rail[Any] {
        
    }
    
    public def getCheckpointValues():Rail[Any] {
        
    } 
    
    public def remake(newPlaces:PlaceGroup, newTeam:Team, newAddedPlaces:ArrayList[Place]) {
        
    }
    
    public def restore(restoreDataMap:HashMap[Any,Any], lastCheckpointIter:Long) {
        
    }
    
    public static def main(args:String[]) {
        val opts = new OptionsParser(args, [
            Option("h","help","this information")
            ], [
            Option("e","spare","number of spare places"),
            Option("k","chkInterval","checkpoint interval"),
            Option("i","iterations","iterations")]);
                                                               
        val spare = opts("e", Long.MAX_VALUE);
        val iterations = opts("i", Long.MAX_VALUE);
        val checkpointInterval = opts("k", -1);
        
        val app = new TestLocalDataStore(iterations);
        val executor = new LocalDataStoreIterativeExecutor(app, spare, checkpointInterval);
        
        executor.run();
    }

}


class LocalDomain {
    public val array:Rail[Long];
    public var curIter:Long = 0;
    public def this (size:Long) {
        array = new Rail[Long](size);
    }
    
    public def add(a:Long) {
        for (var i:Long = 0; i < array.size; i++) {
            array(i) += a;
        }
    }
}
