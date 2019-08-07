package matt.score;

import matt.SpatialObject;

import java.util.List;

public class OneStepResult {
    public int cornerA1, cornerA2, cornerB1, cornerB2;
    public List<SpatialObject> spatialObjects;

    public OneStepResult(int cornerA1, int cornerA2, int cornerB1, int cornerB2, List spatialObjects) {
        this.cornerA1 = cornerA1;
        this.cornerA2 = cornerA2;
        this.cornerB1 = cornerB1;
        this.cornerB2 = cornerB2;
        this.spatialObjects = spatialObjects;
    }
}
