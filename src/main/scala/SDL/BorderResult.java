package SDL;

import java.io.Serializable;

public class BorderResult implements Serializable {
    public int cellInI = 0;
    public int cellInJ = 0;
    public double score = 0;

    public BorderResult(int cellInI, int cellInJ, double score) {
        this.cellInI = cellInI;
        this.cellInJ = cellInJ;
        this.score = score;
    }

    public String makeKey() {
        return String.valueOf(cellInI) + "," + String.valueOf(cellInJ);
    }

    public String getCellInI() {
        return String.valueOf(cellInI);
    }

    public int getCellInJ() {
        return cellInJ;
    }

    public double getScore() {
        return score;
    }

    public boolean check(SpatialObject spatialObject) {
        if (spatialObject.getScore() > score)
            return true;
        return false;
    }

    @Override
    public String toString() {
        return "(" + cellInI + "," + cellInJ + "):" + score;
    }
}
