package SDL.score;

import java.util.List;

import SDL.SpatialObject;

public class ScoreFunctionTotalScore<T extends SpatialObject> extends ScoreFunction<T>{

	//@Override
	public double computeScore(List<T> objects,int start,int end) {
		double totalScore = 0;
		for (int i=start;i<=end;i++) {
			totalScore += objects.get(i).getScore();
		}
		return totalScore;
	}


	@Override
	public double computeScore(List<T> objects) {
		return -1;
	}
}