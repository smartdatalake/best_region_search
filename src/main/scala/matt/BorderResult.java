package matt;

import java.io.Serializable;

public class BorderResult implements Serializable {
    public int nodeNum;
    public double rightScore;
    public double downScore;
    public double cornerScore;

    public BorderResult(int nodeNum, double rightScore, double downScore, double cornerScore) {
        this.nodeNum = nodeNum;
        this.rightScore = rightScore;
        this.downScore = downScore;
        this.cornerScore = cornerScore;
    }
}
