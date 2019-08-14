package matt.score;

import matt.SpatialObject;
import scala.collection.immutable.List;
import scala.collection.mutable.ListBuffer;

import java.io.Serializable;
import java.util.ArrayList;


public class OneStepResult implements Serializable {
    public int cornerALong, cornerALat, cornerBLong, cornerBLat;
    public List<SpatialObject> spatialObjects;

    public OneStepResult() {
    }

    public OneStepResult(int cornerALong, int cornerALat, int cornerBLong, int cornerBLat, List<SpatialObject> spatialObjects) {
        this.cornerALong = cornerALong;
        this.cornerALat = cornerALat;
        this.cornerBLong = cornerBLong;
        this.cornerBLat = cornerBLat;
        this.spatialObjects = spatialObjects;
    }
}
