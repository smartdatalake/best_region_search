package matt.score;

import java.io.Serializable;
import java.util.List;

import matt.SpatialObject;

public abstract class ScoreFunction<T extends SpatialObject> implements Serializable {
	public abstract double computeScore(List<T> objects);
}