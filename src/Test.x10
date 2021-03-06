import x10.io.FileReader;
import x10.io.File;
import x10.io.EOFException;
import x10.util.resilient.map.*;
import x10.util.resilient.map.partition.*;
import x10.util.resilient.map.exception.TransactionAbortedException;
import x10.util.resilient.map.common.*;

public class Test {
    public static def test01() {
        val topology = new Topology();
        val file = new FileReader(new File("topology.txt"));
        var line:String = file.readLine();
        var index:Long = 0;
        try{
            while (line != null) {
                Console.OUT.println(line);
                if (!line.startsWith("#"))
                    topology.addPlace(line,Place(index++));
                line = file.readLine();
            }
        } catch (eof:EOFException) {
            // no more examples
        }
        
        topology.printTopology();
        
        val partitionTable = new PartitionTable(topology.getPlacesCount(), 3);
        partitionTable.createPartitionTable(topology);
        partitionTable.printPartitionTable();
    }
    
    public static def test02() {
        for (p in Place.places()) at (p) async {
            DataStore.getInstance().printTopology();
        }
    }
    
    //succeeded
    public static def test03() {
        val hm = DataStore.getInstance().makeResilientMap("MapA");
        val retryCount = 3; //RETRY COUNT must equal the number of places
        finish for (p in Place.places()) at (p) async {
            for (var i:Long = 0 ; i < retryCount; i++) {
                val txId = hm.startTransaction();
                try{
                    val x = hm.get(txId, "A");
                
                    if (x == null) {
                        Console.OUT.println(here + " ---- will put 0                            =============");
                        hm.put(txId, "A", 0);
                    }
                    else {
                        Console.OUT.println(here + " ---- will put " + ((x as Long)+1) + "   ================");
                        hm.put(txId, "A", (x as Long)+1);
                    }
            
                    hm.commitTransaction(txId);
                    break;
                }
                catch (ex:Exception) {
                    hm.abortTransaction(txId);
                }
            }
        }
        
        Console.OUT.println("Final result = " + hm.get("A") as Long);    
    }
    
    //succeeded
    public static def test04() {
        Console.OUT.println("Test 4 started ...");
        val hm = DataStore.getInstance().makeResilientMap("MapA");
        
        try{
            finish for (p in Place.places()) at (p) async {
                try{
                    val x = hm.get("A");
                    Console.OUT.println("****** " + here + "  x= " + x );
                    
                    if (x == null) {
                        val oldValue = hm.put("A", here.id);
                        Console.OUT.println(here + "PUT SUCCEEDED oldValue["+oldValue+"] ...");
                    }
                    else {
                        hm.put("A", -1);
                        Console.OUT.println(here + "PUT SUCCEEDED (-1) ...");
                    }
                    
                    val x2 = hm.get("A");
                    Console.OUT.println("######## " + here + "  x2= " + x2 );  
                }
                catch (ex:Exception) {                       
                    //ex.printStackTrace();
                }
            }
        }catch(ex:Exception) {
            //ex.printStackTrace();
        }
    }
    
    public static def test05() {
        val hm = DataStore.getInstance().makeResilientMap("MapA");
        
        try{
            hm.put("A", 100);
            val x = hm.get("A");
            if (x == 100) {
                Console.OUT.println("Test succeeded ["+x+"] ...");
            }
            else {
                Console.OUT.println("Test failed  ["+x+"]...");                
            }
        }            
        catch (ex:MultipleExceptions) {
            //ex.printStackTrace();
        }
    }
    
    public static def main(args:Rail[String]) {
        //X10_NPLACES=4 FORCE_ONE_PLACE_PER_NODE=1 ./a.out
        //KILL_PLACE=3 KILL_PLACE_POINT=1 X10_RESILIENT_MODE=1 X10_NPLACES=4 FORCE_ONE_PLACE_PER_NODE=1 ./a.out
        
        //test02();
        //test03();
        //test04();
        test05();
        
    }
}