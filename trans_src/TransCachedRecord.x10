import x10.util.HashMap;

public class TransCachedRecord {
    private val oldValue:Any;
    private var newValue:Any;
	private var readOnly:Boolean = true;

    public def this(oldValue:Any) {
    	this.oldValue = oldValue;
    	this.newValue = oldValue;
    }
    
    public def update(n:Any) {
    	newValue = n;
    	readOnly = false;
    }
    
    public def getOldValue() = oldValue;
    public def getNewValue() = newValue;
    public def readOnly() = readOnly;
}