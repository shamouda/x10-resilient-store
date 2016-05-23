package x10.util.resilient.map.common;

import x10.util.ArrayList;

public class TopologyNode (id:Long, name:String) {
    
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