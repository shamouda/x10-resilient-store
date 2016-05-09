package x10.util.resilient.map.common;

import x10.util.concurrent.AtomicLong;
import x10.util.HashSet;

public class Utils {
    public static val VERBOSE_PLACE = Utils.getEnvLong("VERBOSE_PLACE", -1);
    
    private static val sequence:AtomicLong = new AtomicLong();
    
    public static def getEnvLong(name:String, defaultVal:Long) {
        val env = System.getenv(name);
        val v = (env!=null) ? Long.parseLong(env) : defaultVal;
        return v;
    }
    
    public static def assure(v:Boolean, msg:String) {
        if (!v) {
            val fmsg = "Assertion fail at P" + here.id()+" - "+msg;
            throw new UnsupportedOperationException(fmsg);
        }
    }
    
    public static def getNextTransactionId() {
        val id = sequence.incrementAndGet();
        return (here.id+1)*100000+id;
    }
    
    public static def console(moduleName:String, msg:String) {
        if (VERBOSE_PLACE == -1 || here.id == VERBOSE_PLACE)
            Console.OUT.println(here + " ["+moduleName+"] " + msg);
    }
    
    public static def getDeadReplicas(replicas:HashSet[Long]):HashSet[Long] {
    	val result = new HashSet[Long]();
    	for (x in replicas)
    		if (Place(x).isDead())
    			result.add(x);
    	return result;
    }
    
    
    public static val KILL_PLACE = Utils.getEnvLong("KILL_PLACE", -1);
    public static val KILL_PLACE_POINT = Utils.getEnvLong("KILL_PLACE_POINT", -1);
    public static val POINT_BEGIN_ASYNC_EXEC_REQUEST = 1;
    
    public static def asyncKillPlace() {
    	if (KILL_PLACE == -1 || Place(KILL_PLACE).isDead())
    		return;
    	
    	async at(Place(KILL_PLACE)) {
    		Console.OUT.println("Killing " + here);
    		System.killHere();
    	}
    }
}