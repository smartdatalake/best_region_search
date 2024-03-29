package SDL;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.Serializable;

public class POI extends SpatialObject implements Serializable {

    public POI() {
        super();
    }

    public POI(String id, double x, double y, double score,
               GeometryFactory geometryFactory) {
        super(id, score, geometryFactory.createPoint(new Coordinate(x, y)));
    }

    public Point getPoint() {
        return (Point) getGeometry();
    }

    public void setPoint(Point point) {
        super.setGeometry(point);
    }

    public String toString() {
        return this.getId() + ";name;" + this.getPoint().getX() + ";" + this.getPoint().getY() + ";key;" + getScore();
    }

    public void increaseScore() {
        this.setScore(this.getScore() + 1);
    }
}