package SDL.score;

import SDL.SpatialObject;

import java.util.List;

public class ScoreFunctionCount<T extends SpatialObject> extends ScoreFunction<T> {

    @Override
    public double computeScore(List<T> objects) {
        return objects.size();
    }
}