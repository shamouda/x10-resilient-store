package x10.util.resilient.iterative;

import x10.util.ArrayList;
import x10.util.HashMap;
import x10.util.Team;

public interface LocalDataStoreIterativeApp {
    public def isFinished():Boolean;
    
    public def step():void;
    
    public def getCheckpointAndRestoreKeys():Rail[Any];
    
    public def getCheckpointValues():Rail[Any];    
    
    public def remake(newPlaces:PlaceGroup, newTeam:Team, newAddedPlaces:ArrayList[Place]):void;
    
    public def restore(restoreDataMap:HashMap[Any,Any], lastCheckpointIter:Long):void;   
}
