package matt.score;

import java.util.List;

import matt.SpatialObject;

public class ScoreFunctionCount<T extends SpatialObject> extends ScoreFunction<T> {

	@Override
	public double computeScore(List<T> objects) {
		return objects.size();
	}
}