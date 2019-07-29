package matt.ca;

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

	private HashSet<String> duplicate=new HashSet<>();
	private boolean distinctMode;
	private GeometryFactory geometryFactory;
	public double maxDelete=0;
	public BCAIndexProgressive(boolean distinctMode) {
		super();
		this.distinctMode = distinctMode;
	}

	@Override
	public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k, ScoreFunction<POI> scoreFunction) {
		return findBestCatchmentAreas(pois, eps, k, scoreFunction, new ArrayList<>());
	}

	public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k,
			ScoreFunction<POI> scoreFunction,List<SpatialObject> previous) {
		long time = System.nanoTime();
		if (pois.size() > 10000)
			System.err.print("poi# in a partition " + pois.size() + " on location around   " + pois.get(0).getPoint().getX() + ":" + pois.get(0).getPoint().getY());
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
		if (pois.size() > 10000)
			System.err.println(" time" + TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - time)));
		boolean flag = false;
		for (SpatialObject t : topk)
			if (t.getScore() < maxDelete)
				flag = true;
		if (flag)
			System.err.println("Error: maxDeleted=" + maxDelete + "        result=" + topk.get(0).getScore());
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
			List<SpatialObject> topk,List<SpatialObject> previous) {
		try {
			if (block.type == Block.BLOCK_TYPE_REGION) {
				inspectResult(block, eps, topk, previous);
			} else {
				if (block != null) {

					maxDelete=block.sweep(queue,maxDelete);
//					queue.addAll(newBlocks);

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

	private void inspectResult(Block block, double eps, List<SpatialObject> topk,List<SpatialObject> previous) {
		// generate candidate result
		Envelope e = geometryFactory.createPoint(block.envelope.centre()).getEnvelopeInternal();
		e.expandBy(eps / 2); // with fixed size eps
		// Envelope e = block.envelope; // with tight mbr
		if(block==null)
			return;
		if(block.envelope==null)
			return;
		if(block.envelope.centre()==null)
			return;
		if(duplicate.contains(block.envelope.centre().x + ":" + block.envelope.centre().y+":"+block.type)) {
			block.type=Block.EXPAND_NONE;
			return;
		}
		else
			duplicate.add(block.envelope.centre().x + ":" + block.envelope.centre().y+":"+block.type);

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
			SpatialObject result = new SpatialObject(block.envelope.centre().x + ":" + block.envelope.centre().y, null,
					null, block.utilityScore, geometryFactory.toGeometry(e));

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
}