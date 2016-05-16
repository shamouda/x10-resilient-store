package x10.util.resilient.map.partition;

import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.concurrent.AtomicLong;
import x10.util.resilient.map.common.Utils;
import x10.util.resilient.map.impl.ResilientMapImpl;

/*
 * A topology object contains information about the available nodes and their places.
 * Place 0 communicates with all places to collect the topology information, 
 * then it forwards the topology object to all places. 
 * No changes are expected on the topology during run time.
 * 
 * TODO: support spare nodes
 * */
public class Topology {
	/*List of main nodes*/
    private val nodes:ArrayList[TopologyNode] = new ArrayList[TopologyNode]();
    
    /*The number of main places*/
    private var placesCount:Long = 0;

    /*An atomic sequence used to generate unique ids to nodes*/
    private val sequence = new AtomicLong();
    
    /*List of dead places as known at the leader*/
    private val deadPlaces = new HashSet[Long]();
    
    /*Accessors for private data*/
    public def getNodes() = nodes;    
    public def getPlacesCount() = placesCount;
    public def getNodesCount() = nodes.size();
    
    /*Adds a place to node given the place Id*/
    public def addPlace (nodeName:String, placeId:Long) {
        addPlace(nodeName, Place(placeId));
    }
 
    /*Adds a place to node given the place object*/
    public def addPlace (nodeName:String, place:Place) {
        var node:TopologyNode = getNode(nodeName);
        if (node == null)
            node = addNode(nodeName);
        node.addPlace(place);
        placesCount++;
    }
    
    /*Finds a node given its name*/
    public def getNode(nodeName:String):TopologyNode {
        for (n in nodes) {
            if (n.name.equals(nodeName))
                return n;
        }
        return null;
    }
    
    /*Adds a node given its name*/
    public def addNode (name:String):TopologyNode {
        val node = new TopologyNode(sequence.getAndIncrement(),name);
        nodes.add(node);
        return node;
    }
    
    /*Finds a place given its node index and place index*/
    public def getPlaceByIndex(nodeIndex:Long, placeIndexInsideNode:Long):Place {
        if (nodeIndex < nodes.size()) {
            val node = nodes.get(nodeIndex);
            if (placeIndexInsideNode < node.places.size()){
                return node.places.get(placeIndexInsideNode);
            }
        }
        return Place(-1);
    }
    
    /*Finds the index of a node, given a place in it*/
    public def getNodeIndex(placeId:Long):Long {
        for (var i:Long = 0; i < nodes.size(); i++) {
        	if (nodes.get(i).containsPlace(placeId))
        		return i;
        }
        return -1;
    }
    
    /*Prints the topology*/
    public def printTopology(){
        for (node in nodes) {
            Console.OUT.println(node.toString());
        }
    }
    
    /*Adds a dead place*/
    public def addDeadPlace(pId:Long){
    	deadPlaces.add(pId);
    }
    
    /*Checks if a place is dead*/    
    public def isDeadPlace(pId:Long) {
    	return deadPlaces.contains(pId);
    }
    
    public def clone():Topology {
    	val cloneObj = new Topology();
    	cloneObj.nodes.addAll(nodes);
    	cloneObj.placesCount = placesCount;
        cloneObj.deadPlaces.addAll(deadPlaces);
    	return cloneObj;
    }
    
    public def printDeadPlaces() {
    	for (x in deadPlaces)
    		Console.OUT.println("Topology dead place ["+x+"] ....");	
    }
}

class TopologyNode (id:Long, name:String) {
	
    /*List of node places*/
    public val places:ArrayList[Place] = new ArrayList[Place]();
    
    public def addPlace(x10Place:Place) {
        if (!places.contains(x10Place))
            places.add(x10Place);
    }
    
    public def containsPlace(placeId:Long) {
    	return places.contains(Place(placeId));
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
