package SDL.ca;

import SDL.SpatialObject;
import SDL.score.ScoreFunction;

import java.util.List;

public abstract class BCAFinder<T extends SpatialObject> {
    public abstract List<SpatialObject> findBestCatchmentAreas(List<T> pois, double eps, int topk,
                                                               ScoreFunction<T> scoreFunction);
}