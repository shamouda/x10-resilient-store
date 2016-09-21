package x10.util.resilient.iterative;


import x10.util.Option;
import x10.util.OptionsParser;
import x10.xrx.Runtime;

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

    public def this(app:LocalDataStoreIterativeApp, spare:Long, checkpointInterval:Long) {
        if (x10.xrx.Runtime.RESILIENT_MODE > 0 && checkpointInterval > 1) {
            isResilient = true; 
        }
        this.app = app;
        this.checkpointInterval = checkpointInterval;
    }
    
    public void run() {
        
        
        
        app.init(places, team);
        
        do{
            try {

                restoreIfRequired();
                
                checkpointIfRequired();
                
                try{
                    finish ateach(Dist.makeUnique(places)) {
                        var localIter:Long = 0;
                        while ( !app.isFinished_local() && 
                                (!isResilient || (isResilient && localIter < itersPerCheckpoint)) 
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
                            
                            app.step_local();
                            
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
        }while(restoreRequired || !app.isFinished_local());
        
    }
    
    private def restoreIfRequired() {
        if (restoreRequired) {
            LocalDataStore.getInstance().recoverPlaces(tmpDeadPlaces);
            places = LocalDataStore.getInstance().getPlaces();
            team = new Team(places);
            
            val globTrans = DataStore.startGlobalTransaction(places);
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
        val globTrans = DataStore.startGlobalTransaction(places);
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
