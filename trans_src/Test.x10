import x10.io.FileReader;
import x10.io.File;
import x10.io.EOFException;
import x10.util.resilient.map.*;
import x10.util.resilient.map.partition.*;
import x10.util.resilient.map.exception.TransactionAbortedException;

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
    				topology.addMainPlace(line,Place(index++));
    			line = file.readLine();
    		}
    	} catch (eof:EOFException) {
    	    // no more examples
    	}
    	
    	topology.printTopology();
    	
    	val partitionTable = new PartitionTable(topology, topology.getMainPlacesCount(), 3);
    	partitionTable.createParitionTable();
    	partitionTable.printParitionTable();
	}
	
	public static def test02() {
		for (p in Place.places()) at (p) async {
    		DataStore.getInstance().printTopology();
    	}
	}
	
	public static def test03() {
    	val hm = DataStore.getInstance().makeResilientMap("MapA", 100);
    	finish for (p in Place.places()) at (p) async {
    		val txId = hm.startTransaction();   		
    		try{
    			val x = hm.get(txId, "A");
    			
    			if (x == null)
    				hm.put(txId, "A", 0);
    			else
    				hm.put(txId, "A", (x as Long)+1);
    		
    			hm.commitTransaction(txId);
    		}
    		catch (ex:Exception) {
    			hm.abortTransaction(txId);
    		}
    	}
    	
    	//Console.OUT.println(hm.get("A") as Long);	
	}
	
	
	public static def test04() {
    	val hm = DataStore.getInstance().makeResilientMap("MapA", 100);
    	
		try{
			finish for (p in Place.places()) at (p) async {
				try{
					val x = hm.get("A");
    			
					
    				if (x == null) {
    					hm.put("A", here.id);
    				}
    				else {
    					hm.put("A", -1);
    				}
    				
    				val x2 = hm.get("A");
    				Console.OUT.println("######## " + here + "  x2= " + x2 );  
				}    		
				catch (ex:Exception) {
					Console.OUT.println("######## " + here + "   " + ex.getMessage());    			
				}
			}
		}catch(ex:Exception) {
			Console.OUT.println("@@@@@@@@@@@@@@@@@@@@@@@@@@ " + here + "   " + ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	public static def test05() {
    	val hm = DataStore.getInstance().makeResilientMap("MapA", 100);
    	
		try{
			hm.put("A", 100);

			Console.OUT.println("=================== (1-after put)");
		    val x = hm.get("A");
		    Console.OUT.println("=================== (2-after get)");
		    
			if (x == null || x == 100) {
				Console.OUT.println("Test failed  ["+x+"]...");
			}
			else {
				Console.OUT.println("Test succeeded ["+x+"] ...");
			}
			
		}    		
		catch (ex:Exception) {
			Console.OUT.println("######## " + here + "   " + ex.getMessage());    			
		}
	}
	
    public static def main(args:Rail[String]) {
    	test05();
    }
}