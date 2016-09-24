package x10.util.resilient.iterative;

import x10.util.Team;
import x10.util.HashMap;
import x10.util.Option;
import x10.util.OptionsParser;
import x10.xrx.Runtime;
import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.*;
import x10.regionarray.Dist;

public class LocalDataStoreIterativeExecutor{
    
    private val app:LocalDataStoreIterativeApp;
    private val isResilient:Boolean;
    private val checkpointInterval:Long;
    
    private var lastCheckpointIter:Long;
    private var restoreJustDone:Boolean = false;
    private var restoreRequired:Boolean = false;
    private var tmpDeadPlaces:ArrayList[Place];
    private var places:PlaceGroup;
    private var team:Team;
    private var resilientMap:ResilientMap;

    private var placeTempData:PlaceLocalHandle[PlaceTempData];

    private val VERBOSE = (System.getenv("EXECUTOR_DEBUG") != null && System.getenv("EXECUTOR_DEBUG").equals("1"));
    private val sparePlaces = System.getenv("X10_SPARE_PLACES") == null? 0 : Long.parseLong(System.getenv("X10_SPARE_PLACES"));
    private val KILL_STEP = (System.getenv("EXECUTOR_KILL_STEP") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_STEP")):-1;
    private val KILL_STEP_PLACE = (System.getenv("EXECUTOR_KILL_STEP_PLACE") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_STEP_PLACE")):-1;
    
    public def this(app:LocalDataStoreIterativeApp, spare:Long, checkpointInterval:Long) {
        if (x10.xrx.Runtime.RESILIENT_MODE > 0 && checkpointInterval > 1) {
            isResilient = true; 
            resilientMap = new ResilientMap(sparePlaces);
            this.checkpointInterval = checkpointInterval;
        }
        else  {
        	isResilient = false;
        	this.checkpointInterval = -1;
        }
        this.app = app;
        this.checkpointInterval = checkpointInterval;
    }
    
    public def run() {
    	places = resilientMap.getActivePlaces();
        team = new Team(places);
    	placeTempData = PlaceLocalHandle.make[PlaceTempData](places, ()=>new PlaceTempData());
        //app.init(places, team);
        
        do{
            try {
                restoreIfRequired();
                
                checkpointIfRequired();
                
                try{
                    finish ateach(Dist.makeUnique(places)) {
                        var localIter:Long = 0;
                        while ( !app.isFinished() && 
                                (!isResilient || (isResilient && localIter < checkpointInterval)) 
                               ) {
                            val tmpIter = placeTempData().globalIter;
                            if (isResilient && KILL_STEP == tmpIter && here.id == KILL_STEP_PLACE){
                                at(Place(0)){
                                    placeTempData().place0KillPlaceTime = Timer.milliTime();
                                    Console.OUT.println("[Hammer Log] Time before killing is ["+placeTempData().place0KillPlaceTime+"] ...");
                                }
                                Console.OUT.println("[Hammer Log] Killing ["+here+"] ...");
                                System.killHere();
                            }

                        
                            placeTempData().place0TimeBeforeStep = Timer.milliTime();
                            
                            app.step();
                            
                            if (here.id == 0)
                                placeTempData().place0TimePerIter.add( (Timer.milliTime()-placeTempData().place0TimeBeforeStep) );
                            
                            placeTempData().globalIter++;
                            
                            localIter++;
                            
                            placeTempData().place0DebuggingTotalIter++;
                        }
                    }
                
                } catch (ex:Exception) {
                    throw ex;
                }
            }
            catch (iterEx:Exception) {
                restoreRequired = true;
                tmpDeadPlaces.add(Place(1));
            }
        }while(restoreRequired || !app.isFinished());
        
    }
    
    private def restoreIfRequired() {
        if (restoreRequired) {
        	resilientMap.recoverDeadPlaces();
            places = resilientMap.getActivePlaces();
            team = new Team(places);
            
            val globTrans = resilientMap.startGlobalTransaction(places);
            for (p in places) at (p) async {
                globTrans.localTransactionStarted();  // talks to the master to create a localTrans  
                
                val chkKeys = app.getCheckpointAndRestoreKeys();
                if (chkKeys != null && chkKeys.size > 0) {
                    val chkValues = app.getCheckpointValues();
                    for (var i:Long = 0; i < chkKeys.size; i++){
                        val restoreDataMap = new HashMap[Any,Any]();
                        val key = chkKeys(i);
                        val value = globTrans.get(key);//talks to the master only
                        restoreDataMap.put(key, value);
                    }
                    
                    app.restore(restoreDataMap, lastCheckpointIter);
                }
                
                globTrans.localTransactionCompleted(); //flushes to slave if there is any writes
            }
        
            globalTrans.commit(); //will throw an exception and rollback if restore failed at one place
        }
    }
    
    private def checkpointIfRequired() {
        if (isResilient && !restoreJustDone) {
        val globTrans = resilientMap.startGlobalTransaction(places);
        try {
            finish for (p in places) at (p) async {
                globTrans.localTransactionStarted();  // talks to the master  
                
                val chkKeys = app.getCheckpointAndRestoreKeys();
                if (chkKeys != null && chkKeys.size > 0) {
                    val chkValues = app.getCheckpointValues();
                    for (var i:Long = 0; i < chkKeys.size; i++){
                        val key = chkKeys(i);
                        val value = chkValues(i);
                        globTrans.put(key, value);   //talks to the master -caches the puts
                    }
                }
                
                globTrans.localTransactionCompleted(); //flushes to slave if there is any writes
            }
        
            globalTrans.commit(); //1) tell the coordinator to commit, tell everyone to commit
        }
        catch(ex:Exception) {
            globalTrans.rollback(); //may throw an exception
            throw ex;
        }
        
            
        }
        else {
            restoreJustDone = false;
        }
    }
    
    
    
    
    
    class PlaceTempData {
    	
    	private val VERBOSE_EXECUTOR_PLACE_LOCAL = (System.getenv("EXECUTOR_PLACE_LOCAL") != null 
                && System.getenv("EXECUTOR_PLACE_LOCAL").equals("1"));
    	
        //used by place hammer
        var place0KillPlaceTime:Long = -1;
        
        var lastCheckpointIter:Long = -1;
        var commitCount:Long = 0;
        
    	val getDataStoreTimes:ArrayList[Long];
        val checkpointTimes:ArrayList[Long];
        val checkpointAgreementTimes:ArrayList[Long];
        val restoreTimes:ArrayList[Long];
        val restoreAgreementTimes:ArrayList[Long];
        val stepTimes:ArrayList[Long];
    
    	var placeMaxGetDataStore:Rail[Long];
        var placeMaxCheckpoint:Rail[Long];
        var placeMaxCheckpointAgreement:Rail[Long];
        var placeMaxRestore:Rail[Long];
        var placeMaxRestoreAgreement:Rail[Long];
        var placeMaxStep:Rail[Long];

    	var placeMinGetDataStore:Rail[Long];
        var placeMinCheckpoint:Rail[Long];
        var placeMinCheckpointAgreement:Rail[Long];
        var placeMinRestore:Rail[Long];
        var placeMinRestoreAgreement:Rail[Long];
        var placeMinStep:Rail[Long];
    	
    	val datastore:ResilientMap;
        
        //used for initializing spare places with the same values from Place0
        private def this(getDataStoreTimes:ArrayList[Long], checkTimes:ArrayList[Long], checkAgreeTimes:ArrayList[Long], 
        		restoreTimes:ArrayList[Long], restoreAgreeTimes:ArrayList[Long],
        		stepTimes:ArrayList[Long], lastCheckpointIter:Long, commitCount:Long){
            this.getDataStoreTimes = getDataStoreTimes;
        	this.checkpointTimes = checkTimes;
            this.checkpointAgreementTimes = checkAgreeTimes;
            this.stepTimes = stepTimes;
            this.restoreTimes = restoreTimes;
            this.restoreAgreementTimes = restoreAgreeTimes;
            this.lastCheckpointIter = lastCheckpointIter;
            this.commitCount = commitCount;            
        }
    
        public def this(){
            this.checkpointTimes = new ArrayList[Long]();
            this.checkpointAgreementTimes = new ArrayList[Long]();
            this.restoreTimes = new ArrayList[Long]();
            this.restoreAgreementTimes = new ArrayList[Long]();
            this.stepTimes = new ArrayList[Long]();
            this.getDataStoreTimes = new ArrayList[Long]();
        }
        
        private def getDataStore():ResilientMap{
            return datastore;
        }  
        
    }
}



/*
        val appPlaces = 10;
        val sparePlaces = Place.numPlaces() - appPlaces;
        
        val vPlaces:List[VirtualPlace] = DataStore.getAppPlaces();
        
        
        for (p in vPlaces) at (p.physicalPlace) async {
            //checkpoint
            if (checkpointIter()) {
                transObj = DataStore.startLocalTrans()
                transObj.
                transObj.put(k1,v1);
                transObj.put(k2,v2);
                
                val agreeVal = agree();
                
                transObj.commitLocalTrans();
                or 
                transObj.rollbackLocalTrans();
            }
        }
        
        
        transObj = DataStore.startGlobalTrans()
        
        for (p in vPlaces) at (p.physicalPlace) async {
            //checkpoint
            if (checkpointIter()) {
                transObj.localTransactionStarted();
                transObj.put(k1,v1);
                transObj.put(k2,v2);

                transObj.localTransactionCompleted(); // does not commit
            }
        }
        
        transObj.commitGlobalTrans();
        or
        transObj.rollbackGlobalTrans();
*/
