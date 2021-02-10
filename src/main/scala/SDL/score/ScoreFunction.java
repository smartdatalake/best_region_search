package SDL.score;

import SDL.SpatialObject;

import java.io.Serializable;
import java.util.List;

public abstract class ScoreFunction<T extends SpatialObject> implements Serializable {
    public abstract double computeScore(List<T> objects);
}