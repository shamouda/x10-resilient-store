package x10.util.resilient.localstore;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.resilient.map.common.Utils;
import x10.compiler.Ifdef;
import x10.util.resilient.map.transaction.
public class LocalTransaction (plh:PlaceLocalHandle[LocalDataStore], id:Long) {
    private val putLog:HashMap[String,Any] = new HashMap[String,Any]();    
    
    public def put(key:String, value:Any):Any {
        return plh().masterStore.put(id, key, value);
    }
    
    public def get(key:String):Any {
        return plh().masterStore.get(id, key);
    }
    

    public def commit() {
        val map = plh().masterStore.getTransUpdateLog(id:Long);
        commitSlave();
        
        plh().masterStore.commit(id);
    }
    
    public def commitSlave() {
        
    }
        
    public def rollback() {
        plh().masterStore.rollback(id);
    }
}