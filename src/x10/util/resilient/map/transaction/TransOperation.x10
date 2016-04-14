package x10.util.resilient.map.transaction;

import x10.util.HashMap;
import x10.util.resilient.map.common.Utils;

public class TransOperation {
    public static val GET:Long = 0;
    
    public static val CREATE:Long = 1;
    public static val UPDATE:Long = 2;
    public static val DELETE:Long = 3;
    
    public static def isConflicting (op1:Long, op2:Long) = (isWriteOp(op1) || isWriteOp(op2));
    
    private static def isWriteOp (op:Long) = (op == CREATE || op == UPDATE || op == DELETE);
}