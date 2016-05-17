package x10.util.resilient.map.partition;

import x10.util.concurrent.AtomicInteger;

/*A container for a value and its version*/
public class VersionValue {
    
    private val version:AtomicInteger = new AtomicInteger(-1n);
    private var value:Any;
   
    /*Update the value and increment its version*/
    public def updateValue(newValue:Any):Int {
        val newVersion = version.incrementAndGet();
        value = newValue;
        return newVersion;
    }
    
    public def getVersion() = version.get();
    public def getValue() = value;
    public def toString() {
        return "{ver="+getVersion() +":val="+getValue()+"}";
    }
}