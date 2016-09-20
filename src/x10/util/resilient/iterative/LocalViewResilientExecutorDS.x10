/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 *  (C) Copyright Sara Salem Hamouda 2014-2016.
 */

package x10.util.resilient.iterative;

import x10.util.Timer;

import x10.util.Random;
import x10.regionarray.Dist;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.Team;
import x10.util.GrowableRail;
import x10.util.RailUtils;
import x10.util.resilient.map.ResilientMap;
import x10.util.resilient.map.DataStore;
import x10.xrx.Runtime;
/*
 * TODO:
 * -> maximum retry for restore failures
 * -> support more than 1 place failure.  when a palce dies, store.rebackup()
 * -> no need to notify place death for collectives
 * */
public class LocalViewResilientExecutorDS {
    private var placeTempData:PlaceLocalHandle[PlaceTempData];
    private transient var places:PlaceGroup;
    private var team:Team;
    private val itersPerCheckpoint:Long;
    private var isResilient:Boolean = false;
    // if step() are implicitly synchronized, no need for a step barrier inside the executor
    private val implicitStepSynchronization:Boolean; 
    private val VERBOSE = (System.getenv("EXECUTOR_DEBUG") != null 
                        && System.getenv("EXECUTOR_DEBUG").equals("1"));
    
    //parameters for killing places at different times
    private val simplePlaceHammer:SimplePlaceHammer;  
    private val HAMMER_STEPS = System.getenv("EXECUTOR_KILL_STEPS");
    private val HAMMER_PLACES = System.getenv("EXECUTOR_KILL_PLACES");
    // index of the checkpoint first checkpoint (0), second checkpoint (1), ...etc    
    private val KILL_CHECKVOTING_INDEX = (System.getenv("EXECUTOR_KILL_CHECKVOTING") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_CHECKVOTING")):-1;
    private val KILL_CHECKVOTING_PLACE = (System.getenv("EXECUTOR_KILL_CHECKVOTING_PLACE") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_CHECKVOTING_PLACE")):-1;   
    private val KILL_RESTOREVOTING_INDEX = (System.getenv("EXECUTOR_KILL_RESTOREVOTING") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_RESTOREVOTING")):-1;
    private val KILL_RESTOREVOTING_PLACE = (System.getenv("EXECUTOR_KILL_RESTOREVOTING_PLACE") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_RESTOREVOTING_PLACE")):-1;   
    
    private val DISABLE_ULFM_AGREEMENT = System.getenv("DISABLE_ULFM_AGREEMENT") != null && System.getenv("DISABLE_ULFM_AGREEMENT").equals("1");
   
    
    private transient var runTime:Long = 0;
    private transient var remakeTimes:ArrayList[Double] = new ArrayList[Double]();
    private transient var reconstructTeamTimes:ArrayList[Double] = new ArrayList[Double]();
    private transient var failureDetectionTimes:ArrayList[Double] = new ArrayList[Double]();
    private transient var applicationInitializationTime:Long = 0;
    
    private transient var restoreRequired:Boolean = false;
    private transient var remakeRequired:Boolean = false;
    
    private val CHECKPOINT_OPERATION = 1;
    private val RESTORE_OPERATION = 2;
    
    class PlaceTempData {
    	
    	private val VERBOSE_EXECUTOR_PLACE_LOCAL = (System.getenv("EXECUTOR_PLACE_LOCAL") != null 
                && System.getenv("EXECUTOR_PLACE_LOCAL").equals("1"));
    	
        //used by place hammer
        var place0KillPlaceTime:Long = -1;
        
        var lastCheckpointIter:Long = -1;
        var commitCount:Long = 0;
        
        val checkpointTimes:ArrayList[Long];
        val checkpointAgreementTimes:ArrayList[Long];
        val restoreTimes:ArrayList[Long];
        val restoreAgreementTimes:ArrayList[Long];
        val stepTimes:ArrayList[Long];
    
        var placeMaxCheckpoint:Rail[Long];
        var placeMaxCheckpointAgreement:Rail[Long];
        var placeMaxRestore:Rail[Long];
        var placeMaxRestoreAgreement:Rail[Long];
        var placeMaxStep:Rail[Long];

        var placeMinCheckpoint:Rail[Long];
        var placeMinCheckpointAgreement:Rail[Long];
        var placeMinRestore:Rail[Long];
        var placeMinRestoreAgreement:Rail[Long];
        var placeMinStep:Rail[Long];
    	
    	val datastore:ResilientMap;
        
        //used for initializing spare places with the same values from Place0
        private def this(datastore:ResilientMap, checkTimes:ArrayList[Long], checkAgreeTimes:ArrayList[Long], 
        		restoreTimes:ArrayList[Long], restoreAgreeTimes:ArrayList[Long],
        		stepTimes:ArrayList[Long], lastCheckpointIter:Long, commitCount:Long){
            this.checkpointTimes = checkTimes;
            this.checkpointAgreementTimes = checkAgreeTimes;
            this.stepTimes = stepTimes;
            this.restoreTimes = restoreTimes;
            this.restoreAgreementTimes = restoreAgreeTimes;
            this.lastCheckpointIter = lastCheckpointIter;
            this.commitCount = commitCount;
            this.datastore = datastore;
        }
    
        public def this(datastore:ResilientMap){
            this.checkpointTimes = new ArrayList[Long]();
            this.checkpointAgreementTimes = new ArrayList[Long]();
            this.restoreTimes = new ArrayList[Long]();
            this.restoreAgreementTimes = new ArrayList[Long]();
            this.stepTimes = new ArrayList[Long]();
            this.datastore = datastore;
        }
        
        private def getDataStore():ResilientMap{
            return datastore;
        }  
        
    }
    
    public def this(itersPerCheckpoint:Long, places:PlaceGroup, implicitStepSynchronization:Boolean) {
        this.places = places;
        this.itersPerCheckpoint = itersPerCheckpoint;
        this.implicitStepSynchronization = implicitStepSynchronization;
        this.simplePlaceHammer = new SimplePlaceHammer(HAMMER_STEPS, HAMMER_PLACES);
        if (itersPerCheckpoint > 0 && x10.xrx.Runtime.RESILIENT_MODE > 0) {
            isResilient = true;            
            if (!DISABLE_ULFM_AGREEMENT && !x10.xrx.Runtime.x10rtAgreementSupport()){
            	throw new UnsupportedOperationException("This executor requires an agreement algorithm from the transport layer ...");
        	}
            if (VERBOSE){         
            	Console.OUT.println("HAMMER_STEPS="+HAMMER_STEPS);
            	Console.OUT.println("HAMMER_PLACES="+HAMMER_PLACES);
            	Console.OUT.println("EXECUTOR_KILL_CHECKVOTING="+KILL_CHECKVOTING_INDEX);
            	Console.OUT.println("EXECUTOR_KILL_CHECKVOTING_PLACE="+KILL_CHECKVOTING_PLACE);
            	Console.OUT.println("EXECUTOR_KILL_RESTOREVOTING_INDEX"+KILL_RESTOREVOTING_INDEX);
            	Console.OUT.println("EXECUTOR_KILL_RESTOREVOTING_PLACE"+KILL_RESTOREVOTING_PLACE);
            }
        }
    }

    public def run(app:LocalViewResilientIterativeAppDS) {
        run(app, Timer.milliTime());
    }
    
    //the startRunTime parameter is added to allow the executor to consider 
    //any initlization time done by the application before starting the executor  
    public def run(app:LocalViewResilientIterativeAppDS, startRunTime:Long) {
    	Console.OUT.println("LocalViewResilientExecutorDS: Application start time ["+startRunTime+"] ...");
        applicationInitializationTime = Timer.milliTime() - startRunTime;
        val root = here;
        var ds:ResilientMap = null;
        if (isResilient) {
            ds = DataStore.getInstance().makeResilientMap("ds");
            Console.OUT.println("LocalViewResilientExecutorDS: data store created ...");
        }
        val datastore = ds;
        placeTempData = PlaceLocalHandle.make[PlaceTempData](places, ()=>new PlaceTempData(datastore));
        team = new Team(places);
        var globalIter:Long = 0;
        
        do{
            try {
            	/*** Starting a Restore Operation *****/
                if (remakeRequired) {
                    if (placeTempData().lastCheckpointIter > -1) {
                        if (VERBOSE) Console.OUT.println("Restoring to iter " + placeTempData().lastCheckpointIter);
                        val startRemake = Timer.milliTime();                        
                        val restorePGResult = PlaceGroupBuilder.createRestorePlaceGroup(places);
                        val newPG = restorePGResult.newGroup;
                        val addedPlaces = restorePGResult.newAddedPlaces;
                        
                        if (VERBOSE){
                            var str:String = "";
                            for (p in newPG)
                                str += p.id + ",";
                            Console.OUT.println("Restore places are: " + str);
                        } 
                        val startTeamCreate = Timer.milliTime(); 
                        Console.OUT.println("***********Before Team creation  ndead=["+Place.numDead()+"]...");
                        Runtime.x10rtProbe();
                        Console.OUT.println("***********Before Team creation after probe ndead=["+Place.numDead()+"]...");
                        team = new Team(newPG);
                        Console.OUT.println("***********Team created ...");
                        reconstructTeamTimes.add( Timer.milliTime() - startTeamCreate);
                        app.remake(newPG, team, addedPlaces);
                        Console.OUT.println("***********App remake ...");
                        ///////////////////////////////////////////////////////////
                        //Initialize the new places with the same info at place 0//
                        val lastCheckIter = placeTempData().lastCheckpointIter;
                        val tmpPlace0CheckpointTimes = placeTempData().checkpointTimes;
                        val tmpPlace0CheckpointAgreeTimes = placeTempData().checkpointAgreementTimes;
                        val tmpPlace0RestoreTimes = placeTempData().restoreTimes;
                        val tmpPlace0RestoreAgreeTimes = placeTempData().restoreAgreementTimes;
                        val tmpPlace0StepTimes = placeTempData().stepTimes;
                        val tmpPlace0CommitCount = placeTempData().commitCount;
                        
                        
                        for (sparePlace in addedPlaces){
                            Console.OUT.println("LocalViewResilientExecutor: Adding place["+sparePlace+"] ...");           
                            PlaceLocalHandle.addPlace[PlaceTempData](placeTempData, sparePlace, 
                            		()=>new PlaceTempData(datastore,tmpPlace0CheckpointTimes, tmpPlace0CheckpointAgreeTimes, 
                            				tmpPlace0RestoreTimes, tmpPlace0RestoreAgreeTimes, tmpPlace0StepTimes, lastCheckIter,
                            				tmpPlace0CommitCount));
                        }
                        ///////////////////////////////////////////////////////////
                        places = newPG;
                        globalIter = lastCheckIter;
                        remakeRequired = false;
                        restoreRequired = true;
                        remakeTimes.add(Timer.milliTime() - startRemake) ;                        
                        Console.OUT.println("LocalViewResilientExecutor: All remake steps completed successfully ...");
                    } else {
                        throw new UnsupportedOperationException("process failure occurred but no valid checkpoint exists!");
                    }
                }
                
                Console.OUT.println("LocalViewResilientExecutor: remakeRequired["+remakeRequired+"] restoreRequired["+restoreRequired+"] ...");           
                
                //to be copied to all places
                val tmpRestoreRequired = restoreRequired;
                val tmpGlobalIter = globalIter;
                val placesCount = places.size();
                finish ateach(Dist.makeUnique(places)) {
                    var localIter:Long = tmpGlobalIter;
                    var localRestoreJustDone:Boolean = false;
                    var localRestoreRequired:Boolean = tmpRestoreRequired;
                    
                    Console.OUT.println(here + " ---------- Inside Finish AT-EACH   localRestoreRequired["+localRestoreRequired+"]  ----------");
                
                    while ( !app.isFinished() || localRestoreRequired) {
                    	var stepStartTime:Long = -1; // (-1) is used to differenciate between checkpoint exceptions and step exceptions
                        try{
                        	/**Local Restore Operation**/
                        	if (localRestoreRequired){
                        		checkpointRestoreProtocol(RESTORE_OPERATION, app, team, root, placesCount);
                        		localRestoreRequired = false;
                        		localRestoreJustDone = true;
                        	}
                        	
                        	/**Local Checkpointing Operation**/
                        	if (!localRestoreJustDone) {
                                //take new checkpoint only if restore was not done in this iteration
                                if (isResilient && (localIter % itersPerCheckpoint) == 0) {
                                    if (VERBOSE) Console.OUT.println("["+here+"] checkpointing at iter " + localIter);
                                    checkpointRestoreProtocol(CHECKPOINT_OPERATION, app, team, root, placesCount);
                                    placeTempData().lastCheckpointIter = localIter;
                                }
                            } else {
                            	localRestoreJustDone = false;
                            }
                        	
                        	if (isResilient && simplePlaceHammer.sayGoodBye(localIter)){
                        		executorKillHere("step()");
                        	}

                        	stepStartTime = Timer.milliTime();
                        	if (!implicitStepSynchronization){
                        	    //to sync places & also to detect DPE
                        	    team.barrier();
                        	}
                            app.step();
                            
                            placeTempData().stepTimes.add(Timer.milliTime()-stepStartTime);
                            
                            localIter++;
                            
                        } catch (ex:Exception) {
                            throw ex;
                        }//step catch block
                    }//while !isFinished
                }//finish ateach
            }
            catch (iterEx:Exception) {            	
            	iterEx.printStackTrace();
            	//exception from finish_ateach  or from restore
            	if (isResilient && containsDPE(iterEx)){
            		remakeRequired = true;
            		Console.OUT.println("[Hammer Log] Time DPE discovered is ["+Timer.milliTime()+"] ...");
                    if (placeTempData().place0KillPlaceTime != -1){
                    	failureDetectionTimes.add(Timer.milliTime() - placeTempData().place0KillPlaceTime);
                    	placeTempData().place0KillPlaceTime =  -1;
                        //FIXME: currently we are only able to detect failure detection time only when we kill places
                    }
            	}
            }
        }while(remakeRequired || !app.isFinished());
        
        val runTime = (Timer.milliTime() - startRunTime);
        calculateTimingStatistics();
        
        val averageSteps                   = avergaMaxMinRails(placeTempData().placeMinStep,                placeTempData().placeMaxStep);
        var averageCheckpoint:Rail[Double] = null;
        var averageCheckpointAgreement:Rail[Double] = null;
        var averageRestore:Rail[Double] = null;
        var averageRestoreAgreement:Rail[Double] = null;
        if (isResilient){
            if (placeTempData().checkpointTimes.size() > 0)
                averageCheckpoint          = avergaMaxMinRails(placeTempData().placeMinCheckpoint,          placeTempData().placeMaxCheckpoint);
            
            if (placeTempData().checkpointAgreementTimes.size() > 0)
                averageCheckpointAgreement = avergaMaxMinRails(placeTempData().placeMinCheckpointAgreement, placeTempData().placeMaxCheckpointAgreement);
            
            if (placeTempData().restoreTimes.size() > 0)
                averageRestore             = avergaMaxMinRails(placeTempData().placeMinRestore,             placeTempData().placeMaxRestore);
            
            if ( placeTempData().restoreAgreementTimes.size() > 0)
                averageRestoreAgreement    = avergaMaxMinRails(placeTempData().placeMinRestoreAgreement,    placeTempData().placeMaxRestoreAgreement);
        }
        
        Console.OUT.println("=========Detailed Statistics============");
        Console.OUT.println("Steps-place0:" + railToString(placeTempData().stepTimes.toRail()));
        
        Console.OUT.println("Steps-avg:" + railToString(averageSteps));
    	Console.OUT.println("Steps-min:" + railToString(placeTempData().placeMinStep));
    	Console.OUT.println("Steps-max:" + railToString(placeTempData().placeMaxStep));
    	
    	if (isResilient){
	    	Console.OUT.println("CheckpointData-avg:" + railToString(averageCheckpoint));
	    	Console.OUT.println("CheckpointData-min:" + railToString(placeTempData().placeMinCheckpoint));
	    	Console.OUT.println("CheckpointData-max:" + railToString(placeTempData().placeMaxCheckpoint));
	    	
	    	Console.OUT.println("CheckpointAgree-avg:" + railToString(averageCheckpointAgreement));
	    	Console.OUT.println("CheckpointAgree-min:" + railToString(placeTempData().placeMinCheckpointAgreement));
	    	Console.OUT.println("CheckpointAgree-max:" + railToString(placeTempData().placeMaxCheckpointAgreement));
	    	
	    	Console.OUT.println("RestoreData-avg:" + railToString(averageRestore));
	    	Console.OUT.println("RestoreData-min:" + railToString(placeTempData().placeMinRestore));
	    	Console.OUT.println("RestoreData-max:" + railToString(placeTempData().placeMaxRestore));
	    	
	    	Console.OUT.println("RestoreAgree-avg:" + railToString(averageRestoreAgreement));
	    	Console.OUT.println("RestoreAgree-min:" + railToString(placeTempData().placeMinRestoreAgreement));
	    	Console.OUT.println("RestoreAgree-max:" + railToString(placeTempData().placeMaxRestoreAgreement));
	    	
	    	
	    	Console.OUT.println("FailureDetection-place0:" + railToString(failureDetectionTimes.toRail()));
	    	Console.OUT.println("Remake-place0:" + railToString(remakeTimes.toRail()));
	    	Console.OUT.println("TeamReconstruction-place0:" + railToString(reconstructTeamTimes.toRail()));
    	}
        Console.OUT.println("=========Totals by averaging Min/Max statistics============");
        Console.OUT.println(">>>>>>>>>>>>>>Initialization:"      + applicationInitializationTime);
        Console.OUT.println();
        Console.OUT.println("   ---AverageSingleStep:" + railAverage(averageSteps));
        Console.OUT.println(">>>>>>>>>>>>>>TotalSteps:"+ railSum(averageSteps) );
        Console.OUT.println();
        if (isResilient){
        Console.OUT.println("CheckpointData:"             + railToString(averageCheckpoint));
        Console.OUT.println("   ---AverageCheckpointData:" + railAverage(averageCheckpoint) );
        Console.OUT.println("CheckpointAgreement:"             + railToString(averageCheckpointAgreement)  );
        Console.OUT.println("   ---AverageCheckpointAgreement:" + railAverage(averageCheckpointAgreement)  );
        Console.OUT.println(">>>>>>>>>>>>>>TotalCheckpointing:"+ (railSum(averageCheckpoint)+railSum(averageCheckpointAgreement) ) );
  
        Console.OUT.println();
        Console.OUT.println("Failure Detection:"        + railToString(failureDetectionTimes.toRail()) );
        Console.OUT.println("   ---Failure Detection:"   + railAverage(failureDetectionTimes.toRail()) );
        Console.OUT.println("Remake:"                   + railToString(remakeTimes.toRail()) );
        Console.OUT.println("   ---Remake:"              + railAverage(remakeTimes.toRail()) );
        Console.OUT.println("TeamReconstruction:"      + railToString(reconstructTeamTimes.toRail()) );
        Console.OUT.println("   ---TeamReconstruction:" + railAverage(reconstructTeamTimes.toRail()) );
        Console.OUT.println("RestoreData:"         + railToString(averageRestore));
        Console.OUT.println("   ---RestoreData:"    + railAverage(averageRestore));
        Console.OUT.println("RestoreAgreement:"         + railToString(averageRestoreAgreement) );
        Console.OUT.println("   ---RestoreAgreement:"    + railAverage(averageRestoreAgreement) );
        Console.OUT.println(">>>>>>>>>>>>>>TotalRecovery:" + (railSum(failureDetectionTimes.toRail()) + railSum(remakeTimes.toRail()) + railSum(averageRestore) + railSum(averageRestoreAgreement) ));
        }
        Console.OUT.println("=============================");
        Console.OUT.println("Actual RunTime:" + runTime);
        var calcTotal:Double = applicationInitializationTime + railSum(averageSteps);
        
        if (isResilient){
        	calcTotal += (railSum(averageCheckpoint)+railSum(averageCheckpointAgreement) ) + 
                (railSum(failureDetectionTimes.toRail()) + railSum(remakeTimes.toRail()) + railSum(averageRestore) + railSum(averageRestoreAgreement) );
        }
        Console.OUT.println("Calculated RunTime based on Averages:" + calcTotal 
            + "   ---Difference:" + (runTime-calcTotal));
        
        Console.OUT.println("=========Counts============");
        Console.OUT.println("StepCount:"+averageSteps.size);
        if (isResilient){
            Console.OUT.println("CheckpointCount:"+(averageCheckpoint==null?0:averageCheckpoint.size));
            Console.OUT.println("RestoreCount:"+(averageRestore==null?0:averageRestore.size));
            Console.OUT.println("RemakeCount:"+remakeTimes.size());
            Console.OUT.println("FailureDetectionCount:"+failureDetectionTimes.size());
        
            if (VERBOSE){
                var str:String = "";
                for (p in places)
                    str += p.id + ",";
                Console.OUT.println("List of final survived places are: " + str);            
            }
            Console.OUT.println("=============================");
        }
    }
    
    private def calculateTimingStatistics(){
    	finish for (place in places) at(place) async {
    	    ////// step times ////////
    	    val stpCount = placeTempData().stepTimes.size();
    		placeTempData().placeMaxStep = new Rail[Long](stpCount);
    		placeTempData().placeMinStep = new Rail[Long](stpCount);
    		val dst2max = placeTempData().placeMaxStep;
    		val dst2min = placeTempData().placeMinStep;
    	    team.allreduce(placeTempData().stepTimes.toRail(), 0, dst2max, 0, stpCount, Team.MAX);
    	    team.allreduce(placeTempData().stepTimes.toRail(), 0, dst2min, 0, stpCount, Team.MIN);

    	    if (x10.xrx.Runtime.RESILIENT_MODE > 0n){
        		////// checkpoint times ////////
        		val chkCount = placeTempData().checkpointTimes.size();
        		if (chkCount > 0) {
        		    placeTempData().placeMaxCheckpoint = new Rail[Long](chkCount);
        		    placeTempData().placeMinCheckpoint = new Rail[Long](chkCount);
        		    val dst1max = placeTempData().placeMaxCheckpoint;
        		    val dst1min = placeTempData().placeMinCheckpoint;
        		    team.allreduce(placeTempData().checkpointTimes.toRail(), 0, dst1max, 0, chkCount, Team.MAX);
        		    team.allreduce(placeTempData().checkpointTimes.toRail(), 0, dst1min, 0, chkCount, Team.MIN);
        		}
	    	    ////// restore times ////////
	    	    val restCount = placeTempData().restoreTimes.size();
	    	    if (restCount > 0) {
	    	        placeTempData().placeMaxRestore = new Rail[Long](restCount);
	    	        placeTempData().placeMinRestore = new Rail[Long](restCount);
	    	        val dst3max = placeTempData().placeMaxRestore;
	    	        val dst3min = placeTempData().placeMinRestore;
	    	        team.allreduce(placeTempData().restoreTimes.toRail(), 0, dst3max, 0, restCount, Team.MAX);
	    	        team.allreduce(placeTempData().restoreTimes.toRail(), 0, dst3min, 0, restCount, Team.MIN);
	    	    }
	    	    ////// checkpoint agreement times ////////
	    	    val chkAgreeCount = placeTempData().checkpointAgreementTimes.size();
	    	    if (chkAgreeCount > 0) {
	    	        placeTempData().placeMaxCheckpointAgreement = new Rail[Long](chkAgreeCount);
	    	        placeTempData().placeMinCheckpointAgreement = new Rail[Long](chkAgreeCount);
	    	        val dst4max = placeTempData().placeMaxCheckpointAgreement;
	    	        val dst4min = placeTempData().placeMinCheckpointAgreement;
	    	        team.allreduce(placeTempData().checkpointAgreementTimes.toRail(), 0, dst4max, 0, chkAgreeCount, Team.MAX);
	    	        team.allreduce(placeTempData().checkpointAgreementTimes.toRail(), 0, dst4min, 0, chkAgreeCount, Team.MIN);
	    	    }
	    	    
	    	    ////// restore agreement times ////////
	    	    val restAgreeCount = placeTempData().restoreAgreementTimes.size();
	    	    if (restAgreeCount > 0) {
	    		    placeTempData().placeMaxRestoreAgreement = new Rail[Long](restAgreeCount);
	    		    placeTempData().placeMinRestoreAgreement = new Rail[Long](restAgreeCount);
	    		    val dst5max = placeTempData().placeMaxRestoreAgreement;
	    		    val dst5min = placeTempData().placeMinRestoreAgreement;
	    	        team.allreduce(placeTempData().restoreAgreementTimes.toRail(), 0, dst5max, 0, restAgreeCount, Team.MAX);
	    	        team.allreduce(placeTempData().restoreAgreementTimes.toRail(), 0, dst5min, 0, restAgreeCount, Team.MIN);
	    	    }
    	    }
        }
    }
    
    private def processIterationException(ex:Exception) {
        if (ex instanceof DeadPlaceException) {
            ex.printStackTrace();
            if (!isResilient) {
                throw ex;
            }
        }
        else if (ex instanceof MultipleExceptions) {
            val mulExp = ex as MultipleExceptions;
            if (isResilient) {                
                val filtered = mulExp.filterExceptionsOfType[DeadPlaceException]();
                if (filtered != null) throw filtered;
                val deadPlaceExceptions = mulExp.getExceptionsOfType[DeadPlaceException]();
                for (dpe in deadPlaceExceptions) {
                    dpe.printStackTrace();
                }
            } else {
                throw mulExp;
            }
        }
        else
            throw ex;
    }
    
    private def containsDPE(ex:Exception):Boolean{
    	if (ex instanceof DeadPlaceException)
    		return true;
    	if (ex instanceof MultipleExceptions) {
            val mulExp = ex as MultipleExceptions;
            val deadPlaceExceptions = mulExp.getExceptionsOfType[DeadPlaceException]();
            if (deadPlaceExceptions == null)
            	return false;
            else
            	return true;
        }
    	
    	return false;
    }
    
    //Checkpointing will only occur in resilient mode
    /**
     * lastCheckpointIter    needed only for restore
     * */
    private def checkpointRestoreProtocol(operation:Long, app:LocalViewResilientIterativeAppDS, team:Team, root:Place, placesCount:Long){
    	val op:String = (operation==CHECKPOINT_OPERATION)?"Checkpoint":"Restore";
    
        val startOperation = Timer.milliTime();
        val excs = new GrowableRail[CheckedThrowable]();
        val datastore = placeTempData().getDataStore();
        val txId = datastore.startTransaction();
        
        var vote:Long = 1;
        try{
            if (operation == CHECKPOINT_OPERATION) {
            	val chkKeys = app.getCheckpointAndRestoreKeys();
            	if (chkKeys != null && chkKeys.size > 0) {
            	    val chkValues = app.getCheckpointValues();
            	    for (var i:Long = 0; i < chkKeys.size; i++){
                        val key = chkKeys(i);
                        val value = chkValues(i);
                        datastore.put(txId, key, value);
                    }
            	}
            }
            else {
                val restoreDataMap = new HashMap[Any,Any]();
                val chkKeys = app.getCheckpointAndRestoreKeys();
                if (chkKeys != null && chkKeys.size > 0) {
                	for (var i:Long = 0; i < chkKeys.size; i++) {
                        val key = chkKeys(i);
                	    val value = datastore.get(txId, key);
                	    restoreDataMap.put(key, value);
                    }
                }                
            	app.restore(restoreDataMap, placeTempData().lastCheckpointIter);
            }
            
            //see if we can commit
            if (!datastore.prepareCommit(txId))
    	        vote = 0;
            
            if (VERBOSE) Console.OUT.println(here+" Succeeded in operation ["+op+"]  myVote["+vote+"]");
        }catch(ex:Exception){
            vote = 0;
            excs.add(ex);
            Console.OUT.println("["+here+"]  EXCEPTION MESSAGE while "+op+" = " + ex.getMessage());
            ex.printStackTrace();
        }
        
        if (operation == CHECKPOINT_OPERATION)
            placeTempData().checkpointTimes.add(Timer.milliTime() - startOperation);
        else if (operation == RESTORE_OPERATION)
        	placeTempData().restoreTimes.add(Timer.milliTime() - startOperation);
        
        
        if ((operation == CHECKPOINT_OPERATION && KILL_CHECKVOTING_INDEX == placeTempData().checkpointTimes.size() && 
               here.id == KILL_CHECKVOTING_PLACE) || 
               (operation == RESTORE_OPERATION    && KILL_RESTOREVOTING_INDEX == placeTempData().restoreTimes.size() && 
         	   here.id == KILL_RESTOREVOTING_PLACE)) {
            executorKillHere(op);
        }
        
        val startAgree = Timer.milliTime();
        try{
        	if (VERBOSE) Console.OUT.println(here+" Starting agree call in operation ["+op+"]");
        	var success:Int = 1N;
        	if (!DISABLE_ULFM_AGREEMENT)
        	    success = team.agree((vote as Int));
        	if (success == 1N) {
        		if (VERBOSE) Console.OUT.println(here+" Agreement succeeded in operation ["+op+"] transId["+txId+"] ...");
        		datastore.confirmCommit(txId);
        	}
        	else {
        		//Failure due to a reason other than place failure, will need to abort.
        		throw new Exception("[Fatal Error] Agreement failed in operation ["+op+"]   success = ["+success+"]");
        	}
        }
        catch(agrex:Exception){
        	Console.OUT.println(here + " [Fatal Error] Agreement failed in operation ["+op+"] ");
        	//agrex.printStackTrace();
        	excs.add(agrex);
        	try {
        	    datastore.abortTransaction(txId);
        	}catch(dummyEx:Exception) {
        	    
        	}
        }
        if (operation == CHECKPOINT_OPERATION)
            placeTempData().checkpointAgreementTimes.add(Timer.milliTime() - startAgree);
        else if (operation == RESTORE_OPERATION)
        	placeTempData().restoreAgreementTimes.add(Timer.milliTime() - startAgree);
        	
        if (excs.size() > 0){
        	throw new MultipleExceptions(excs);
        }
    }
    
    private def executorKillHere(op:String) {
    	at(Place(0)){
			placeTempData().place0KillPlaceTime = Timer.milliTime();
            Console.OUT.println("[Hammer Log] Time before killing is ["+placeTempData().place0KillPlaceTime+"] ...");
		}
		Console.OUT.println("[Hammer Log] Killing ["+here+"] before "+op+" ...");
		System.killHere();
    }
    
    public def railToString[T](r:Rail[T]):String {
        if (r == null)
            return "";
    	var str:String = "";
        for (x in r)
        	str += x + ",";
        return str;
    }
    
    public def railSum(r:Rail[Double]):Double {
        if (r == null)
            return 0.0;
    	return RailUtils.reduce(r, (x:Double, y:Double) => x+y, 0.0);
    }
    
    
    public def railAverage(r:Rail[Double]):Double {
        if (r == null)
            return 0.0;
        val railAvg = railSum(r) / r.size;        
    	return  Math.round(railAvg);
    }
    
    public def avergaMaxMinRails[T](max:Rail[T], min:Rail[T]):Rail[Double] {
        val result = new Rail[Double](max.size);
        for (i in 0..(max.size-1)){
        	result(i) = (max(i) as Double + min(i) as Double)/2.0;
        	result(i) = ((result(i)*100) as Long)/100.0;  //two decimal points only
        }
        return result;
    }
}

class SimplePlaceHammer {
	val map:HashMap[Long,Long] = new HashMap[Long,Long]();
	public def this(steps:String, places:String) {
	    if (steps != null && places != null) {
	        val sRail = steps.split(",");
		    val pRail = places.split(",");
		
		    if (sRail.size == pRail.size) {
		    	for (var i:Long = 0; i < sRail.size ; i++) {
		    		map.put(Long.parseLong(sRail(i)), Long.parseLong(pRail(i)));
		    	}
		    }
	    }
	}
	
	public def sayGoodBye(curStep:Long):Boolean {
		val placeToKill = map.getOrElse(curStep,-1);
		if (placeToKill == here.id)
			return true;
		else
			return false;
	}
}


/*Test commands:
==> Kill place during a step:

simple test  spare-restore:
===========================
X10_NUM_IMMEDIATE_THREADS=1 \
X10_EXIT_BY_SIGKILL=1 \
EXECUTOR_KILL_STEP=15 \
EXECUTOR_KILL_STEP_PLACE=7 \
X10_PLACE_GROUP_RESTORE_MODE=1 \
X10_RESILIENT_MODE=1 \
X10_RESILIENT_STORE_MODE=1 \
mpirun -np 9 -am ft-enable-mpi \
--mca errmgr_rts_hnp_proc_fail_xcast_delay 0 \
--mca orte_base_help_aggregate 0 \
bin/lulesh2.0 -e 1 -k 100 -s 10 -i 50 -p



X10_ASYMMETRIC_IMMEDIATE_THREAD=true \


simple test  non resilient:
===========================
mpirun -np 8 \
--mca orte_base_help_aggregate 0 \
bin/lulesh2.0 -s 10 -i 50 -p


mpirun -np 64 -am ft-enable-mpi \
--mca errmgr_rts_hnp_proc_fail_xcast_delay 0 \
--mca orte_base_help_aggregate 0 \
bin/lulesh2.0 -s 10 -i 50 -p




X10_EXIT_BY_SIGKILL=1 \
X10RT_MPI_DEBUG_PRINT=0 \
X10_RESILIENT_VERBOSE=0 \
EXECUTOR_KILL_STEP=15 \
EXECUTOR_KILL_STEP_PLACE=7 \
X10_RESILIENT_STORE_VERBOSE=0 \
X10_TEAM_DEBUG_INTERNALS=1 \
X10_PLACE_GROUP_RESTORE_MODE=1 \
EXECUTOR_DEBUG=1 \
X10_RESILIENT_MODE=1 \
X10_ASYMMETRIC_IMMEDIATE_THREAD=true \
mpirun -np 9 -am ft-enable-mpi \
--mca errmgr_rts_hnp_proc_fail_xcast_delay 0 \
--mca orte_base_help_aggregate 0 \
bin/lulesh2.0 -e 1 -k 10 -s 10 -i 50 -p


X10RT_MPI_PROBE_SLEEP_MICROSECONDS=10000 \
X10RT_MPI_DEBUG_PROBE_PLACE=80 \
X10RT_MPI_DEBUG_SEND_RECV_PLACE=8 \
X10_RESILIENT_VERBOSE=0 \
X10RT_MPI_DEBUG_PRINT=0 \
EXECUTOR_KILL_STEP=15 \
EXECUTOR_KILL_STEP_PLACE=2 \
EXECUTOR_KILL_RESTOREVOTING=0 \
EXECUTOR_KILL_RESTOREVOTING_PLACE=6 \
X10_RESILIENT_STORE_VERBOSE=1 \
X10_TEAM_DEBUG_INTERNALS=1 \
X10_PLACE_GROUP_RESTORE_MODE=1 \
X10_ASYMMETRIC_IMMEDIATE_THREAD=true \
X10_NUM_IMMEDIATE_THREADS=2 \
EXECUTOR_DEBUG=1 \
X10_RESILIENT_MODE=1 \
mpirun -np 10 -am ft-enable-mpi \
--mca errmgr_rts_hnp_proc_fail_xcast_delay 0 \
bin/lulesh2.0 -e 2 -k 10 -s 10 -i 50 -p




X10_EXIT_BY_SIGKILL=1 \
EXECUTOR_KILL_STEP=15 \
EXECUTOR_KILL_STEP_PLACE=7 \
X10_PLACE_GROUP_RESTORE_MODE=1 \
X10_RESILIENT_MODE=1 \
X10_ASYMMETRIC_IMMEDIATE_THREAD=true \
mpirun -np 9 -am ft-enable-mpi \
--mca errmgr_rts_hnp_proc_fail_xcast_delay 0 \
--mca orte_base_help_aggregate 0 \
bin/lulesh2.0 -e 1 -k 10 -s 10 -i 50 -p


==> Kill place before checkpoint voting:

EXECUTOR_KILL_CHECKVOTING=1 \
EXECUTOR_KILL_CHECKVOTING_PLACE=3 \
EXECUTOR_KILL_STEP=5 \
EXECUTOR_KILL_STEP_PLACE=3 \
X10_RESILIENT_STORE_VERBOSE=1 \
X10_TEAM_DEBUG_INTERNALS=0 \
X10_PLACE_GROUP_RESTORE_MODE=1 \
EXECUTOR_DEBUG=1 \
X10_RESILIENT_MODE=1 \
mpirun -np 9 -am ft-enable-mpi \
--mca errmgr_rts_hnp_proc_fail_xcast_delay 0 \
bin/lulesh2.0 -e 1 -k 3 -s 10 -i 10 -p

*/
