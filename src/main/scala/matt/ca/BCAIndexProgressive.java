package matt.ca;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import matt.Grid;
import matt.POI;
import matt.SpatialObject;
import matt.score.ScoreFunction;

public class BCAIndexProgressive extends BCAFinder<POI> {

	private HashSet<String> duplicate = new HashSet<>();
	private boolean distinctMode;
	private GeometryFactory geometryFactory;

	public BCAIndexProgressive(boolean distinctMode) {
		super();
		this.distinctMode = distinctMode;
	}

	@Override
	public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k, ScoreFunction<POI> scoreFunction) {
		return findBestCatchmentAreas(pois, eps, k, scoreFunction, new ArrayList<>());
	}

	public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k,
													  ScoreFunction<POI> scoreFunction, List<SpatialObject> previous) {
		long time = System.nanoTime();
		boolean timed=false;
		if (pois.size() > 5000) {
			System.err.println("poi# in a partition " + pois.size() + " on location around   " + pois.get(0).getPoint().getX() + ":" + pois.get(0).getPoint().getY());
			timed = true;
		}
		if (pois.size() > 5000) {
			HashMap<String, POI> temp = new HashMap<>();
			for (POI poi : pois) {
				double x = myRound(poi.getPoint().getX(),1000);
				double y =  myRound(poi.getPoint().getY(),1000);
				if (temp.containsValue(x + ":" + y)) {
					temp.get(x + ":" + y).increaseScore();
				}
				else
					temp.put(x + ":" + y,poi);
			}
	//		System.err.println("before "+ pois.size());
	//		System.err.println(temp.size());
			pois=new ArrayList<>();
			pois.addAll(temp.values());
	//		System.err.println("after "+pois.size());
		}
	//	System.err.println("Now start with pois# "+ pois.size());
		List<SpatialObject> topk = new ArrayList<SpatialObject>();
		if (pois.size() == 0) {
			SpatialObject t = new SpatialObject();
			t.setScore(0);
			topk.add(t);
			return topk;
		}
		geometryFactory = new GeometryFactory(new PrecisionModel(), pois.get(0).getPoint().getSRID());
		Grid grid = new Grid(pois, eps);
		PriorityQueue<Block> queue = initQueue(grid, scoreFunction, eps);
		grid = null;
		Block block;
		while (topk.size() < k && !queue.isEmpty()) {
			block = queue.poll();
			processBlock(block, eps, scoreFunction, queue, topk, previous);
			block = null;
		}
		queue = null;
		if (topk.size() == 0) {
			SpatialObject t = new SpatialObject();
			t.setScore(0);
			topk.add(t);
		}
		if (timed)
			System.err.println(" time" + TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - time)));
		return topk;
	}

	public PriorityQueue<Block> initQueue(Grid grid, ScoreFunction<POI> scoreFunction, double eps) {

		PriorityQueue<Block> queue = new PriorityQueue<Block>();

		List<POI> cellPois;
		Block block;
		Envelope cellBoundary;
		/* Iterate over the cells and compute an upper bound for each cell. */
		for (Integer row : grid.getCells().keySet()) {
			for (Integer column : grid.getCells().get(row).keySet()) {

				cellBoundary = grid.cellIndexToGeometry(row, column, geometryFactory).getEnvelopeInternal();
				cellBoundary.expandBy(0.5 * eps);

				cellPois = new ArrayList<POI>();
				for (int i = row - 1; i <= row + 1; i++) {
					for (int j = column - 1; j <= column + 1; j++) {
						if (grid.getCells().get(i) != null && grid.getCells().get(i).get(j) != null) {
							for (POI p : grid.getCells().get(i).get(j)) {
								if (cellBoundary.intersects(p.getPoint().getCoordinate())) {
									cellPois.add(p);
								}
							}
						}
					}
				}

				block = new Block(cellPois, scoreFunction, Block.BLOCK_TYPE_CELL, Block.BLOCK_ORIENTATION_VERTICAL,
						Block.EXPAND_NONE, eps, geometryFactory);

				queue.add(block);
			}
		}
		return queue;
	}

	private void processBlock(Block block, double eps, ScoreFunction<POI> scoreFunction, PriorityQueue<Block> queue,
							  List<SpatialObject> topk, List<SpatialObject> previous) {
		try {
			if (block.type == Block.BLOCK_TYPE_REGION) {
				inspectResult(block, eps, topk, previous);
			} else {
				if (block != null) {

					List<Block> newBlocks = block.sweep();
					queue.addAll(newBlocks);

				}
			}
			if ((block.type == Block.BLOCK_TYPE_SLAB || block.type == Block.BLOCK_TYPE_REGION) && block.pois.size() > 1) {
				Block[] derivedBlocks = block.getSubBlocks();
				for (int i = 0; i < derivedBlocks.length; i++) {
					queue.add(derivedBlocks[i]);
				}
			}
		} catch (OutOfMemoryError e) {
			System.err.println("current block location:   " + block.envelope.centre().getX() + ":" + block.envelope.centre().getY());
			System.err.println("current block size:   " + block.pois.size());
			System.err.println("queue size:   " + queue.size());
			int recCNT = 0;
			for (Block b : queue)
				recCNT += b.orderedRectangles.size();
			System.err.println("recCNT:   " + recCNT);

		}
	}

	private void inspectResult(Block block, double eps, List<SpatialObject> topk, List<SpatialObject> previous) {
		// generate candidate result
		Envelope e = geometryFactory.createPoint(block.envelope.centre()).getEnvelopeInternal();
		e.expandBy(eps / 2); // with fixed size eps
		// Envelope e = block.envelope; // with tight mbr
		if (block == null)
			return;
		if (block.envelope == null)
			return;
		if (block.envelope.centre() == null)
			return;
		if (duplicate.contains(myRound(block.envelope.centre().x,10000) + ":" + myRound(block.envelope.centre().y,10000) + ":" + block.type)) {
			block.type = Block.EXPAND_NONE;
			return;
		} else
			duplicate.add(myRound(block.envelope.centre().x,10000) + ":" + myRound(block.envelope.centre().y,10000) + ":" + block.type);

		// if this result is valid, add it to top-k
		boolean isDistinct = true;
		if (distinctMode) {
			for (SpatialObject so : topk) {
				if (e.intersects(so.getGeometry().getEnvelopeInternal())) {
					isDistinct = false;
					break;
				}
			}
			for (SpatialObject so : previous) {
				if (e.intersects(so.getGeometry().getEnvelopeInternal())) {
					isDistinct = false;
					break;
				}
			}
		}
		if (isDistinct) {
			SpatialObject result = new SpatialObject(block.envelope.centre().x + ":" + block.envelope.centre().y,  block.utilityScore, geometryFactory.toGeometry(e));
			topk.add(result);
		}
	}

	@SuppressWarnings("unused")
	private void removeOverlappingPoints(Block block, List<SpatialObject> topk) {
		/*
		 * Check if any existing results overlap with this block. If so, remove common
		 * points.
		 */
		List<SpatialObject> overlappingResults = new ArrayList<SpatialObject>(topk);
		Envelope border = block.envelope;
		overlappingResults.removeIf(p -> !border.intersects(p.getGeometry().getEnvelopeInternal()));

		for (SpatialObject r : overlappingResults) {
			block.pois.removeIf(p -> r.getGeometry().covers(p.getPoint()));
		}
	}

	public boolean getDistinctMode() {
		return distinctMode;
	}

	public void setDistinctMode(boolean distinctMode) {
		this.distinctMode = distinctMode;
	}


	public static int myRound(double n, double resolution) { // use 1000, 5000
		return (int) (n*resolution);
	}

}