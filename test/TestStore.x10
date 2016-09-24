import harness.x10Test;

import x10.util.resilient.localstore.ResilientMap;

import x10.util.Option;
import x10.util.OptionsParser;

public class TestStore(spare:Long,iterations:Long,checkpointInterval:Long,vi:Long,vp:Long) extends x10Test {
    
    public def run(): Boolean {
        val resilientMap = ResilientMap.make(spare);
        
        var restoreRequired:Boolean = false;
        var places:PlaceGroup = resilientMap.getActivePlaces();
        var curIter:Long = 0;
        do {
            try{
                
                if (restoreRequired) {
                    resilientMap.recoverDeadPlaces();
                    restoreRequired = true;
                    places = resilientMap.getActivePlaces();
                }
                
                
                finish for (p in places) at (p) async {
                    for (var i:Long = 0; i < iterations; i++) {
                        val localTrans = resilientMap.startLocalTransaction();
                        
                        localTrans.put("X", i);
                        localTrans.put("Y", here.id);
                        
                        localTrans.get("X");
                        localTrans.get("Y");
                        
                        localTrans.commit();
                    }
                }
                curIter+= iterations;
            }
            catch(ex:Exception) {
                ex.printStackTrace();
                restoreRequired = true;
            }
            
        }
        while (curIter < iterations || restoreRequired);
        
        
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
