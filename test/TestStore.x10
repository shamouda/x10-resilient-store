import harness.x10Test;

import x10.util.resilient.localstore;

import x10.util.Option;
import x10.util.OptionsParser;

public class TestStore(spare:Long,iterations:Long,checkpointInterval:Long,vi:Long,vp:Long)  {
    
    public def run(): Boolean {
        val resilientMap = new ResilientMap(spare);
        
        var restoreRequired:Boolean = false;
        var curIter:Long = 0;
        var places:PlaceGroup = resilientMap.getActivePlaces();
        do {
            try{
                
                if (restoreRequired) {
                    resilientMap.recoverDeadPlaces();
                    restoreRequired = true;
                    places = resilientMap.getActivePlaces();
                }
                
                
                finish for (p in places) at (p) async {
                    for (var i:Long = curIter; i < iterations; i++) {
                        val localTrans = resilientMap.startLocalTransaction();
                        
                        localTrans.put("X", i);
                        localTrans.put("Y", here.id);
                        
                        localTrans.get("X");
                        localTrans.get("Y");
                        
                        localTrans.commit();
                    }
                }
            }
            catch(ex:Exception) {
                ex.printStackTrace();
                restoreRequired = true;
            }
            
        }
        while (curIter < iterations || restoreRequired);
        
        
    }
    
    public static def main(args:Rail[String]) {
        val opts = new OptionsParser(args, [
            Option("h","help","this information")
            ], [
            Option("e","spare","number of spare places"),
            Option("k","chkInterval","checkpoint interval"),
            Option("i","iterations","iterations",
            Option("vi","victim_iteration","victim_iteration",
            Option("vp","victim_place","victim_place")]);
                                                               
        val spare = opts("e", Long.MAX_VALUE);
        val iterations = opts("i", Long.MAX_VALUE);
        val checkpointInterval = opts("k", -1);
        val vi = opts("vi", -1);
        val vp = opts("vp", -1);
        
        new TestStore(spare,iterations,checkpointInterval,vi,vp).execute();
    }

}
