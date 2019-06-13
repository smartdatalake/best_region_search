package matt.ca;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import matt.ObjectSizeFetcher;
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
		List<SpatialObject> topk = new ArrayList<SpatialObject>();
		if(pois.size()==0){
			SpatialObject t=new SpatialObject();
			t.setScore(0);
			topk.add(t);
			return topk;
		}
	//	System.err.println("pois:"+ObjectSizeFetcher.getObjectSize(pois));
		geometryFactory = new GeometryFactory(new PrecisionModel(), pois.get(0).getPoint().getSRID());
		System.err.println("pois size:::"+pois.size());
		Grid grid = new Grid(pois, eps);
		PriorityQueue<Block> queue = initQueue(grid, scoreFunction, eps);
		grid=null;
		Block block;
		System.err.println("queue initial size:::"+queue.size());
		System.gc ();
		System.runFinalization ();
		while (topk.size() < k && !queue.isEmpty()) {
			block = queue.poll();
			processBlock(block, eps, scoreFunction, queue, topk,previous);
			block=null;
		}
		System.err.println("queue ending size:::"+queue.size());
		queue=null;
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
		if (block.type == Block.BLOCK_TYPE_REGION) {
			inspectResult(block, eps, topk,previous);
		} else {
			List<Block> newBlocks = block.sweep();
			queue.addAll(newBlocks);
		}
		if ((block.type == Block.BLOCK_TYPE_SLAB || block.type == Block.BLOCK_TYPE_REGION) && block.pois.size() > 1) {
			Block[] derivedBlocks = block.getSubBlocks();
			for (int i = 0; i < derivedBlocks.length; i++) {
				queue.add(derivedBlocks[i]);
			}
		}
	}

	private void inspectResult(Block block, double eps, List<SpatialObject> topk,List<SpatialObject> previous) {
		// generate candidate result
		Envelope e = geometryFactory.createPoint(block.envelope.centre()).getEnvelopeInternal();
		e.expandBy(eps / 2); // with fixed size eps
		// Envelope e = block.envelope; // with tight mbr
		/*if(duplicate.contains(block.envelope.centre().x + ":" + block.envelope.centre().y)) {
			block.type=Block.EXPAND_NONE;
			return;
		}
		else
			duplicate.add(block.envelope.centre().x + ":" + block.envelope.centre().y);*/

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