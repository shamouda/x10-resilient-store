import x10.io.FileReader;
import x10.io.File;
import x10.io.EOFException;

public class Tester {
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
    	
    	val partitionTable = new PartitionTable(topology, 3);
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
    		catch (ex:TransactionAbortedException) {
    			ex.printStackTrace();
    		}
    		catch (ex:Exception) {
    			hm.abortTransaction(txId);
    		}
    	}
    	
    	//Console.OUT.println(hm.get("A") as Long);	
	}
	
    public static def main(args:Rail[String]) {
    	val hm = DataStore.getInstance().makeResilientMap("MapA", 100);
    	finish for (p in Place.places()) at (p) async {  		
    		try{
    			val x = hm.get("A");
    			
    			if (x == null)
    				hm.put("A", 0);
    			else
    				hm.put("A", (x as Long)+1);
    		}    		
    		catch (ex:Exception) {
    			ex.printStackTrace();
    		}
    	}
    	
    	//Console.OUT.println(hm.get("A") as Long);
    }
}