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
	
    public static def main(args:Rail[String]) {
    	for (p in Place.places()) at (p) async {
    		DataStore.getInstance().printTopology();
    	}
    }
}