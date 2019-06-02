package matt.ca;

import matt.Grid;
import matt.POI;
import matt.SpatialObject;
import matt.score.ScoreFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

public class BCAIndexProgressive2 extends BCAFinder<POI> implements Serializable {

	private boolean distinctMode;
	private GeometryFactory geometryFactory;
	private long overallStartTime, resultEndTime;
	private Grid grid;
	private PriorityQueue<Block> queue;
	ScoreFunction<POI> scoreFunction;
	public BCAIndexProgressive2(List<POI> pois,double eps,ScoreFunction<POI> scoreFunction) {
		super();
		geometryFactory = new GeometryFactory(new PrecisionModel(), pois.get(0).getPoint().getSRID());
		this.distinctMode = true;
		this.scoreFunction=scoreFunction;
		grid = new Grid(pois, eps);
		queue = initQueue(grid, scoreFunction, eps);
	}

	@Override
	public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k, ScoreFunction<POI> scoreFunction) {
		return findBestCatchmentAreas( eps, k, null);
	}

	public List<SpatialObject> findBestCatchmentAreas(double eps,int k, List<SpatialObject> previous) {
		List<SpatialObject> topk = new ArrayList<SpatialObject>();
		Block block;
		while (topk.size() < k && !queue.isEmpty()) {
			block = queue.poll();
			processBlock(block, eps, scoreFunction, queue, topk,previous);
		}
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
		// int pointsBefore = block.pois.size();

		// if (distinctMode) {
		// removeOverlappingPoints(block, topk);
		// }

		// if (block.pois.size() < pointsBefore) {
		// // recreate block and reinsert it in the queue
		// block = new Block(block.pois, scoreFunction, block.type,
		// block.orientation, eps, geometryFactory);
		// queue.add(block);
		// } else {
		if (block.type == Block.BLOCK_TYPE_REGION) {
			inspectResult(block, eps, topk,previous);
		} else {
			List<Block> newBlocks = block.sweep();
			queue.addAll(newBlocks);
		}
		// insert the two derived sub-blocks in the queue
		if ((block.type == Block.BLOCK_TYPE_SLAB || block.type == Block.BLOCK_TYPE_REGION) && block.pois.size() > 1) {
			Block[] derivedBlocks = block.getSubBlocks();
			for (int i = 0; i < derivedBlocks.length; i++) {
				queue.add(derivedBlocks[i]);
			}
		}
		// }
	}

	private void inspectResult(Block block, double eps, List<SpatialObject> topk,List<SpatialObject> previous) {
		// generate candidate result
		Envelope e = geometryFactory.createPoint(block.envelope.centre()).getEnvelopeInternal();
		e.expandBy(eps / 2); // with fixed size eps
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
			result.setAttributes(new HashMap<Object, Object>());
			result.getAttributes().put("coveredPoints", block.pois);

			resultEndTime = (System.nanoTime() - overallStartTime) / 1000000;
			result.getAttributes().put("executionTime", resultEndTime);
			topk.add(result);

			System.out.println("Results so far: " + topk.size());
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