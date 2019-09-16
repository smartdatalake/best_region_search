package matt.score;

import matt.SpatialObject;
import scala.collection.immutable.List;

import java.io.Serializable;


public class OneStepResult implements Serializable {
    public int countSafe, countUnsafe, index, minSafe;
    public List<SpatialObject> spatialObjects;

    public OneStepResult() {
    }

    public OneStepResult(int countSafe, int countUnsafe, int index, int minSafe, List<SpatialObject> spatialObjects) {
        this.countSafe = countSafe;
        this.countUnsafe = countUnsafe;
        this.index = index;
        this.minSafe = minSafe;
        this.spatialObjects = spatialObjects;
    }
}
