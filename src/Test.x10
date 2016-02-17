public class Test {
	public static def main(args:Rail[String]) {
	
		val killPlace = 3;
		
		//Initialize all places
		finish for (p in Place.places()) at (p) {
			DataStore.getInstance();
		}
		
		Console.OUT.println("Getting Place0 DataStore instance");
		val ds = DataStore.getInstance();
		Console.OUT.println("Getting Data Store Succeeded ...");
		
		val mapA = ds.makeResilientMap("A", true);  // broadcasts to all data store places
		Console.OUT.println("Created Map A ...");
		val mapB = ds.makeResilientMap("B", false);  //broadcasts to all data store places
		Console.OUT.println("Created Map B");
		
		Console.OUT.println("====================================================");
		finish for (p in Place.places()) at (p) {
			Console.OUT.println(DataStore.getInstance().toString());
		}
		Console.OUT.println("====================================================");
		
		//Create the Resilient Cluster by Adding One Place At a time
		finish for (p in Place.places()) at (p) async {
			mapA.put("name_"+here.id, "Place("+here.id+")" );
			mapB.put("identifier_"+here.id, here.id );
			
			if (here.id == killPlace)
				System.killHere();
			
			val x = mapA.get("name_"+here.id);
			val y = mapB.get("identifier_"+here.id);
			
			Console.OUT.println("X => " + x);
			Console.OUT.println("Y => " + y);
		}
	}
}