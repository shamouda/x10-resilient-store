import harness.x10Test;

import x10.util.resilient.localstore.SPMDResilientMap;

import x10.regionarray.Dist;
import x10.util.Option;
import x10.util.OptionsParser;

public class TestStore(spare:Long,iterations:Long,checkpointInterval:Long,vi:Long,vp:Long) extends x10Test {
    
    public def run(): Boolean {
        val resilientMap = SPMDResilientMap.make(spare);
        var restoreRequired:Boolean = false;
        var restoreJustDone:Boolean = false;
        var places:PlaceGroup = resilientMap.getActivePlaces();
        var lastCheckpointIter:Long = 0;
        var plh:PlaceLocalHandle[AppLocal] = PlaceLocalHandle.make[AppLocal](places, () => new AppLocal() );
        do {
            try{
                
            	restoreJustDone = false;
                if (restoreRequired) {
                    resilientMap.recoverDeadPlaces();
                    places = resilientMap.getActivePlaces();
                    plh = PlaceLocalHandle.make[AppLocal](places, () => new AppLocal());
                    
                    val constPLH = plh;
                    finish ateach(Dist.makeUnique(places)) {
                    	
                    	val trans = resilientMap.startSPMDTransaction();                    	
                    	val v = trans.get("P") as AppLocal;
                    	trans.commit();
                    	//we don't need to agree in restore
                    	constPLH().sum = v.sum;
                    }                    
                    restoreRequired = false;
                    restoreJustDone = true;
                }
            	
                
            	if (!restoreJustDone) {
            		//checkpoint
            		val constPLH = plh;
            		finish ateach(Dist.makeUnique(places)) {
            			if (constPLH() == null)
            				Console.OUT.println("ERROR null at " + here);
            			
            			val trans = resilientMap.startSPMDTransaction();
            			trans.put("P", constPLH());
            			trans.prepare();
            			//agree
            			trans.commit();
            		}
            	}
                
            	val constPLH = plh;
                val startIteration = lastCheckpointIter;
                finish ateach(Dist.makeUnique(places)) {
                    for (var i:Long = startIteration; i < startIteration+checkpointInterval; i++) {
                    	constPLH().sum += (here.id+1) +i;
                    	Console.OUT.println("tmp   " + here + " ======> "+ ((here.id+1) +i));
                    }
                    Console.OUT.println("tmp   " + here + " =======================> "+ constPLH().sum);
                }
                lastCheckpointIter += checkpointInterval;
            }
            catch(ex:Exception) {
                ex.printStackTrace();
                restoreRequired = true;
            }
            
        }
        while (lastCheckpointIter < iterations || restoreRequired);
        
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
            Option("i","iterations","iterations"),
            Option("vi","victim_iteration","victim_iteration"),
            Option("vp","victim_place","victim_place")]);
                                                               
        val spare = opts("e", Long.MAX_VALUE);
        val iterations = opts("i", Long.MAX_VALUE);
        val checkpointInterval = opts("k", -1);
        val vi = opts("vi", -1);
        val vp = opts("vp", -1);
        
        new TestStore(spare,iterations,checkpointInterval,vi,vp).execute();
    }

}


class AppLocal {
	var sum:Long = 0;
}
