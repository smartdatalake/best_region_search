package matt.score;

import matt.SpatialObject;

import java.util.List;

public class OneStepResult {
    public int cornerALong, cornerALat, cornerBLong, cornerBLat;
    public List<SpatialObject> spatialObjects;

    public OneStepResult() {
    }

    public OneStepResult(int cornerALong, int cornerALat, int cornerBLong, int cornerBLat, List spatialObjects) {
        this.cornerALong = cornerALong;
        this.cornerALat = cornerALat;
        this.cornerBLong = cornerBLong;
        this.cornerBLat = cornerBLat;
        this.spatialObjects = spatialObjects;
    }
}
