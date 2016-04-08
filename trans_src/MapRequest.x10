import x10.util.HashSet;
import x10.util.concurrent.SimpleLatch;

public class MapRequest {
	
	public static val REQ_COMMIT:Int = 1n;
	public static val REQ_ABORT:Int = 2n;
	public static val REQ_GET:Int = 3n;
    public static val REQ_PUT:Int = 4n;
    public static val REQ_DELETE:Int = 5n;
    public static val REQ_KEY_SET:Int = 6n;
    
	public val transactionId:Long;
    public val requestType:Int;

    public var inKey:Any;
	public var inValue:Any;
    
    public var success:Boolean;
    public var outValue:Any;
    public var outKeySet:HashSet[Any];
    public var outException:Exception;
    
    public val lock:SimpleLatch;
    
    public def this(transId:Long, reqType:Int) {
    	this.transactionId = transId;
    	this.requestType = reqType;
    	this.lock = new SimpleLatch();
    }
    
    
    public def toString():String {
    	var str:String = "";
        str += "<request  transactionId="+transactionId+"  type="+typeDesc(requestType)+">\n";        
        str += "     <input  key="+(inKey)+"/>\n";
        str += "     <input  value="+(inValue)+"/>\n";
        if (outValue != null)
        	str += "     <output  oldValue="+(outValue)+"/>\n";
        if (outKeySet != null)
        	str += "     <output  keySet="+(outKeySet)+"/>\n";
        if (outException != null)
        	str += "     <output  exception="+(outException)+"/>\n";
        str += "</request>\n";		
    	return str;
    }
    
    private def typeDesc(typeId:Int):String {
    	switch(typeId){
    		case REQ_COMMIT: return "Commit";
    		case REQ_ABORT: return "Abort";
    		case REQ_GET: return "Get";
    		case REQ_PUT: return "Put";
    		case REQ_DELETE: return "Delete";
    		case REQ_KEY_SET: return "KeySet";
    	}
    	return null;
    }
}