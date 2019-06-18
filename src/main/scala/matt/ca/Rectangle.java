package matt.ca;

import matt.POI;

import java.io.Serializable;

public class Rectangle implements Comparable<Rectangle>, Serializable {
	public float idx;
	public boolean isMin;
	public POI p;

	public Rectangle(float idx, boolean isMin, POI p) {
		this.idx = idx;
		this.isMin = isMin;
		this.p = p;
	}

	@Override
	public int compareTo(Rectangle r) {
		if (this.idx < r.idx)
			return -1;
		else if (this.idx == r.idx) {
			if (!this.isMin && r.isMin) {
				return -1;
			} else if (this.isMin && !r.isMin) {
				return 1;
			} else
				return 0;
		} else {
			return 1;
		}
	}
}
