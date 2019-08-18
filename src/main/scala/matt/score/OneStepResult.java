package matt.score;

import matt.SpatialObject;
import scala.collection.immutable.List;

import java.io.Serializable;


public class OneStepResult implements Serializable {
    public int countSafe, countUnsafe, index, cornerBLat;
    public List<SpatialObject> spatialObjects;

    public OneStepResult() {
    }

    public OneStepResult(int countSafe, int countUnsafe, int index, int cornerBLat, List<SpatialObject> spatialObjects) {
        this.countSafe = countSafe;
        this.countUnsafe = countUnsafe;
        this.index = index;
        this.cornerBLat = cornerBLat;
        this.spatialObjects = spatialObjects;
    }
}
