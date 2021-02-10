package SDL.score;

import SDL.SpatialObject;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScoreFunctionDistinctKeywords extends ScoreFunction<SpatialObject> {

    @Override
    public double computeScore(List<SpatialObject> objects) {
        Set<String> distinctKeywords = new HashSet<String>();
        //	for (SpatialObject object : objects) {
        //		distinctKeywords.addAll(object.getKeywords());
        //	}
        return distinctKeywords.size();
    }
}