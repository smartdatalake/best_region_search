package SDL.score;

import java.util.List;

import SDL.SpatialObject;

public class ScoreFunctionCount<T extends SpatialObject> extends ScoreFunction<T> {

	@Override
	public double computeScore(List<T> objects) {
		return objects.size();
	}
}