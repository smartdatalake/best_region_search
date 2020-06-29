package SDL.ca;

import java.util.List;

import SDL.SpatialObject;
import SDL.score.ScoreFunction;

public abstract class BCAFinder<T extends SpatialObject> {
	public abstract List<SpatialObject> findBestCatchmentAreas(List<T> pois, double eps, int topk,
			ScoreFunction<T> scoreFunction);
}