public class Utils {
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
    
    
    public static def console(moduleName:String, msg:String) {
    	Console.OUT.println(here + " ["+moduleName+"] " + msg);
    }
}