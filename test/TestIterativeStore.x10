import harness.x10Test;

import x10.util.resilient.localstore.*;

import x10.regionarray.Dist;
import x10.util.Team;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.util.Option;
import x10.util.OptionsParser;

//EXECUTOR_KILL_STEPS=3 EXECUTOR_KILL_PLACES=3 DISABLE_ULFM_AGREEMENT=1 X10_RESILIENT_MODE=1 X10_NPLACES=6 ./TestIterativeStore.o -e 1 -k 5 -i 10
public class TestIterativeStore(spare:Long,iterations:Long,checkpointInterval:Long) extends x10Test implements SPMDResilientIterativeApp {
    
	var places:PlaceGroup;
    var team:Team;
    var plh:PlaceLocalHandle[AppLocal];

    
    public def isFinished():Boolean {
    	//Console.OUT.println(here+ " -> curItr["+plh().curIter+"]  iterations["+iterations+"]");
    	return plh().curIter == iterations;
    }
    
    public def step() {  
    	val index = places.indexOf(here);
    	plh().sum += (index+1) + plh().curIter;
    	Console.OUT.println(here + " ===> "+ ((index+1) +plh().curIter));
    	plh().curIter++;
    }
    
    public def getCheckpointAndRestoreKeys():Rail[String] {
    	val keys = new Rail[String](1);
    	keys(0) = "P";
    	return keys;
    }
    
    public def getCheckpointValues():Rail[Any] {
    	val values = new Rail[Any](1);
    	values(0) = plh();
    	return values;
    }    
    
    public def remake(newPlaces:PlaceGroup, newTeam:Team, newAddedPlaces:ArrayList[Place]) {
    	val oldPlaces = places;
        PlaceLocalHandle.destroy(oldPlaces, plh, (Place)=>true);
    	
    	this.team = team;
    	this.places = newPlaces;
    	plh = PlaceLocalHandle.make[AppLocal](places, () => new AppLocal());   	 
    }
    
    public def restore(restoreDataMap:HashMap[String,Any], lastCheckpointIter:Long) {
    	val value = restoreDataMap.getOrThrow("P") as AppLocal;
    	plh().sum = value.sum;
    	plh().curIter = value.curIter;
    }
    
	
    public def run(): Boolean {
        val resilientMap = SPMDResilientMap.make(spare);
        places = resilientMap.getActivePlaces();
        plh = PlaceLocalHandle.make[AppLocal](places, () => new AppLocal() );
        val executor = new SPMDResilientIterativeExecutor(checkpointInterval, resilientMap, false);
        executor.run(this); 
        
        
        val constPLH = plh;
        finish ateach(Dist.makeUnique(places)) {
        	Console.OUT.println(here + "=>" + constPLH().sum);
        }
        
        return true;
    }
    
    
    public static def main(args:Rail[String]) {
        val opts = new OptionsParser(args, [
            Option("h","help","this information")
            ], [
            Option("e","spare","number of spare places"),
            Option("k","chkInterval","checkpoint interval"),
            Option("i","iterations","iterations")
            ]);
                                                               
        val spare = opts("e", Long.MAX_VALUE);
        val iterations = opts("i", Long.MAX_VALUE);
        val checkpointInterval = opts("k", -1);        
        
        new TestIterativeStore(spare,iterations,checkpointInterval).execute();
    }

}


class AppLocal {
	var sum:Long = 0;
    var curIter:Long = 0;
}
