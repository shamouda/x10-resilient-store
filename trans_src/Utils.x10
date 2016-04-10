import x10.util.concurrent.AtomicLong;

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
}