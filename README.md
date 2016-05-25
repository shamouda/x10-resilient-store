# x10-resilient-store

This project provides X10 users with a resilient distributed hash map, that can tolerate the loss of multiple places.
The map is accessible to applications through two classes x10.util.resilient.map.DataStore and x10.util.resilient.map.ResilientMap.

The following code is an example for using the resilient store: 
```java
val mapName = “my_map”;
val map = DataStore.getInstance().makeResilientMap(mapName);

for ( p in Place.places()) at (p) async {
    map.put (“some key”, “some value”);
}
``` 

The implementation of the data store is based on a software transactional memory framework.

A user can also perform multiple operations atomically within a single transaction as follows:
```java
for ( p in Place.places()) at (p) async {
    val txId = map.startTransaction();
    val x = map.get(txId, “X”);
    val y = map.get(txId, “Y”);
    map.put (txId, “Z”, x+y);
    map.commitTransaction(txId);
}
```
