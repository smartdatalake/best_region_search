package matt.score;

import java.util.List;

import matt.SpatialObject;

public abstract class ScoreFunction<T extends SpatialObject> {
	public abstract double computeScore(List<T> objects);
}