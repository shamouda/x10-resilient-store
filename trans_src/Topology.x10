import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.concurrent.AtomicLong;

/*
 * The mapping between nodes, places and partitions
 * */
public class Topology {
	private val mainNodes:ArrayList[TopologyNode] = new ArrayList[TopologyNode]();
    //private val spareNodes:ArrayList[TopologyNode] = new ArrayList[TopologyNode]();
    private val sequence = new AtomicLong();
    public def this(){}
    
    public def getMainNodes() = mainNodes;
    
    public def getMainPlacesCount():Long {
    	var count:Long = 0;
    	for (node in mainNodes) {
    		count += node.places.size();
    	}
    	return count;
    }
 
    public def addMainPlace (nodeName:String, place:Place) {
    	var node:TopologyNode = getNode(nodeName);
    	if (node == null)
    		node = addMainNode(nodeName);
    	node.addPlace(place);
    }
    
    public def getNode(nodeName:String):TopologyNode {
    	for (n in mainNodes) {
    		if (n.getName().equals(nodeName))
    			return n;
    	}
    	return null;
    }
    
    public def addMainNode (name:String):TopologyNode {
    	val node = new TopologyNode(sequence.getAndIncrement(),name);
    	mainNodes.add(node);
    	return node;
    }
    
    public def printTopology(){
    	for (node in mainNodes) {
    		Console.OUT.println(node.toString());
    	}
    }
    
}

class TopologyNode {
	private val id:Long;
	private val name:String;
	public val places:ArrayList[Place] = new ArrayList[Place]();

    public def this (id:Long, name:String) {
    	this.id = id;
    	this.name = name;
    }
    
    public def getId() = id;
    public def getName() = name;
    
    public def addPlace(x10Place:Place) {
    	places.add(x10Place);
    }
    
    public def toString():String {
    	var str:String = "";
        str += "<node id="+id+"  name="+name+">\n";
        for (p in places) {
        	str += "   <place id="+p.id+">\n";
        }
        str += "</node>\n";
        return str;
    }
    
}
