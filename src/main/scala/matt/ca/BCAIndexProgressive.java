package matt.ca;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import matt.definitions.Generic;
import matt.definitions.GridIndexer;
import matt.score.ScoreFunctionTotalScore;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import matt.Grid;
import matt.POI;
import matt.SpatialObject;
import matt.score.ScoreFunction;

public class BCAIndexProgressive extends BCAFinder<POI> {

	private HashSet<String> duplicate = new HashSet<>();
	private HashMap<String,SpatialObject> topKIndex;
	private boolean distinctMode;
	private GeometryFactory geometryFactory;
	private GridIndexer gridIndexer;

	public BCAIndexProgressive(boolean distinctMode, GridIndexer gridIndexer) {
		super();
		this.distinctMode = distinctMode;
		this.gridIndexer=gridIndexer;
	}

	@Override
	public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k, ScoreFunction<POI> scoreFunction) {
		return findBestCatchmentAreas(pois, eps, k, (ScoreFunctionTotalScore)scoreFunction, new ArrayList<>(),-1);
	}

	public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k, ScoreFunctionTotalScore<POI> scoreFunction) {
		return findBestCatchmentAreas(pois, eps, k, scoreFunction, new ArrayList<>(),-1);
	}

	public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k,
													  ScoreFunctionTotalScore<POI> scoreFunction, List<SpatialObject> previous,int node) {

		long time = System.nanoTime();
		System.err.print(","+node);
		topKIndex=new HashMap<>();
		for (SpatialObject spatialObject: previous ) {
			topKIndex.put((gridIndexer.getCellIndex(spatialObject.getGeometry().getCoordinates()[1].x
					, spatialObject.getGeometry().getCoordinates()[1].y)._1().toString()+":"+gridIndexer.getCellIndex(spatialObject.getGeometry().getCoordinates()[1].x
					, spatialObject.getGeometry().getCoordinates()[1].y)._2().toString()),spatialObject);
		}
		boolean timed = false;
	//	if (pois.size() > 5000) {
	//		System.err.println("poi# in a partition " + pois.size() + " on location around   " + pois.get(0).getPoint().getX() + ":" + pois.get(0).getPoint().getY());
	//		timed = true;
	//	}
		HashMap<String, POI> temp = new HashMap<>();
		for (POI poi : pois) {
			double x = poi.getPoint().getX();
			double y = poi.getPoint().getY();
			if (temp.containsValue(x + ":" + y)) {
				temp.get(x + ":" + y).increaseScore();
			} else
				temp.put(x + ":" + y, poi);

	//				System.err.println("before "+ pois.size());
	//				System.err.println(temp.size());
			pois = new ArrayList<>();
			pois.addAll(temp.values());
	//				System.err.println("after "+pois.size());
		}
	//		System.err.println("Now start with pois# "+ pois.size());
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
			if (block.envelope != null && block.pois.size() > 0)
				processBlock(block, eps, scoreFunction, queue, topk, previous);
			block = null;
		}
		queue = null;
		if (topk.size() == 0) {
			SpatialObject t = new SpatialObject();
			t.setScore(0);
			topk.add(t);
		}
	//	if (timed)
	//		System.err.println(" time" + TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - time)));
		return topk;
	}

	public PriorityQueue<Block> initQueue(Grid grid, ScoreFunctionTotalScore<POI> scoreFunction, double eps) {

		PriorityQueue<Block> queue = new PriorityQueue<Block>();

	//	 cellPois;
		Block block;
		Envelope cellBoundary;
		/* Iterate over the cells and compute an upper bound for each cell. */
		for (Integer row : grid.getCells().keySet()) {
			for (Integer column : grid.getCells().get(row).keySet()) {

				cellBoundary = grid.cellIndexToGeometry(row, column, geometryFactory).getEnvelopeInternal();
				cellBoundary.expandBy(0.5 * eps);

				List<POI> cellPois = new ArrayList<POI>();
				for (int i = row - 1; i <= row + 1; i++) {
					Map<Integer, List<POI>> hm = grid.getCells().get(i);
					if (grid.getCells().get(i) != null) {
						for (int j = column - 1; j <= column + 1; j++) {
							List<POI> hm2 = hm.get(j);
							if (hm2 != null) {
								for (POI p : hm2) {
									if (cellBoundary.intersects(p.getPoint().getCoordinate())) {
										cellPois.add(p);
									}
								}
							}
						}
					}
				}
		//		block = new Block(cellPois, scoreFunction, Block.BLOCK_TYPE_CELL, Block.BLOCK_ORIENTATION_VERTICAL,
		//				Block.EXPAND_NONE, eps, geometryFactory);
				if (cellPois.size() > 0) {
				block = new Block(cellPois, scoreFunction, Block.BLOCK_TYPE_CELL, Block.BLOCK_ORIENTATION_VERTICAL,
						Block.EXPAND_NONE, eps, geometryFactory,0,cellPois.size()-1);

				queue.add(block);
				}
			}
		}
		return queue;
	}

	private void processBlock(Block block, double eps, ScoreFunction<POI> scoreFunction, PriorityQueue<Block> queue,
							  List<SpatialObject> topk, List<SpatialObject> previous) {
		try {
			if (block.type == Block.BLOCK_TYPE_REGION) {
				inspectResult(block, eps, topk);
			} else {
				if (block != null&&block.orderedRectangles.size()>0) {

					List<Block> newBlocks = block.sweep();
					queue.addAll(newBlocks);

				}
			}
			//if ((block.type == Block.BLOCK_TYPE_SLAB || block.type == Block.BLOCK_TYPE_REGION) && block.pois.size() > 1) {
			if ((block.type == Block.BLOCK_TYPE_SLAB || block.type == Block.BLOCK_TYPE_REGION) && block.start<block.end) {
				Block[] derivedBlocks = block.getSubBlocks();
				for (int i = 0; i < derivedBlocks.length; i++) {
					queue.add(derivedBlocks[i]);
				}
			}
		} catch (OutOfMemoryError e) {
			System.err.println("current block location:   " + block.envelope.centre().getX() + ":" + block.envelope.centre().getY());
		//	System.err.println("current block size:   " + block.pois.size());
			System.err.println("current block size:   " + (block.end-block.start+1));
			System.err.println("queue size:   " + queue.size());
			int recCNT = 0;
			for (Block b : queue)
				recCNT += b.orderedRectangles.size();
			System.err.println("recCNT:   " + recCNT);

		}
	}

	private void inspectResult(Block block, double eps, List<SpatialObject> topk) {
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
		if (duplicate.contains(myRound(block.envelope.centre().x, 10000) + ":" + myRound(block.envelope.centre().y, 10000) + ":" + block.type)) {
			block.type = Block.EXPAND_NONE;
			return;
		} else
			duplicate.add(myRound(block.envelope.centre().x, 10000) + ":" + myRound(block.envelope.centre().y, 10000) + ":" + block.type);
		SpatialObject result = new SpatialObject(block.envelope.centre().y + "," + block.envelope.centre().x, block.relevanceScore, geometryFactory.toGeometry(e));
		// if this result is valid, add it to top-k
		boolean isDistinct = true;
		int cellI = Integer.parseInt(gridIndexer.getCellIndex(result.getGeometry().getCoordinates()[1].x
				, result.getGeometry().getCoordinates()[1].y)._1().toString());
		int cellJ = Integer.parseInt(gridIndexer.getCellIndex(result.getGeometry().getCoordinates()[1].x
				, result.getGeometry().getCoordinates()[1].y)._2().toString());
		for (int i = -1; i <= 1; i++) {
			for (int j = -1; j <= 1; j++) {
				SpatialObject t = topKIndex.getOrDefault(((cellI + i) + ":" + (cellJ + j)), null);
				if (t != null && Generic.intersects(result, t)) {
					isDistinct = false;
				}
			}
		}
		if (isDistinct) {
			if (result.getGeometry() != null) {
				topk.add(result);
				topKIndex.put((gridIndexer.getCellIndex(result.getGeometry().getCoordinates()[1].x
						, result.getGeometry().getCoordinates()[1].y)._1().toString() + ":" + gridIndexer.getCellIndex(result.getGeometry().getCoordinates()[1].x
						, result.getGeometry().getCoordinates()[1].y)._2().toString()), result);
			}
		}
	}

	@SuppressWarnings("unused")
/*	private void removeOverlappingPoints(Block block, List<SpatialObject> topk) {
		*//*
		 * Check if any existing results overlap with this block. If so, remove common
		 * points.
		 *//*
		List<SpatialObject> overlappingResults = new ArrayList<SpatialObject>(topk);
		Envelope border = block.envelope;
		overlappingResults.removeIf(p -> !border.intersects(p.getGeometry().getEnvelopeInternal()));

		for (SpatialObject r : overlappingResults) {
			block.pois.removeIf(p -> r.getGeometry().covers(p.getPoint()));
		}
	}*/

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