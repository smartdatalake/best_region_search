package matt;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

public class SpatialObject implements Comparable<SpatialObject> , Serializable {
	private String id;
	private double score;
	private  Geometry geometry;
	private int part=-1;

	public SpatialObject() {
		
	}
	
	public SpatialObject(String id, double score, Geometry geometry) {
		super();
		this.id = (id != null && id.length() != 0) ? id : UUID.randomUUID().toString();
		this.score = score;
		this.geometry = geometry;
	}
	@Override
	public String toString(){
		return String.valueOf(score)+"id:"+id+"part:"+part;
	}
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return "";
	}


	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public Geometry getGeometry() {
		return geometry;
	}

	public void setGeometry(Geometry geometry) {
		this.geometry = geometry;
	}

	public static <T extends SpatialObject> Envelope getEnvelope(List<T> objects) {
		GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(),
				objects.get(0).getGeometry().getSRID());
		Geometry[] geometries = new Geometry[objects.size()];
		for (int i = 0; i < geometries.length; i++) {
			geometries[i] = objects.get(i).getGeometry();
		}
		return geometryFactory.createGeometryCollection(geometries).getEnvelopeInternal();
	}

	public int getPart() {
		return part;
	}

	public void setPart(int part) {
		this.part = part;
	}

	@Override
	public int compareTo(SpatialObject o) {
		if (this.getScore() > o.getScore()) {
			return -1;
		} else if (this.getScore() == o.getScore()) {
			return 0;
		} else {
			return 1;
		}
	}
}