package matt.ca;

import java.util.List;

import matt.SpatialObject;
import matt.score.ScoreFunction;

public abstract class BCAFinder<T extends SpatialObject> {
	public abstract List<SpatialObject> findBestCatchmentAreas(List<T> pois, double eps, int topk,
			ScoreFunction<T> scoreFunction);
}