package matt.ca;

import matt.*;
import matt.definitions.Generic;
import matt.definitions.GridIndexer;
import matt.score.OneStepResult;
import matt.score.ScoreFunction;
import matt.score.ScoreFunctionTotalScore;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import scala.Tuple2;

import java.util.*;


public class BCAIndexProgressiveOneRoundRedHybrid {
	private boolean IsOptimized;
	private boolean distinctMode;
	private GeometryFactory geometryFactory;
	private GridIndexer gridIndexer;
	private int opt1 = 0;
	private HashMap<String, Double> border;
	private int overall = 0;
	private int unsafeCNT = 0;
	private SpatialObject left, up, corner;
	private int node;
	private HashSet<String> duplicate = new HashSet<>();
	private HashMap<String,SpatialObject> topKIndex;
	public BCAIndexProgressiveOneRoundRedHybrid(boolean distinctMode, GridIndexer gridIndexer) {
		super();
		this.distinctMode = distinctMode;
		this.gridIndexer = gridIndexer;
	}


	public OneStepResult findBestCatchmentAreas(List<POI> pois, int node, double eps, int k, ScoreFunctionTotalScore<POI> scoreFunction,List<SpatialObject> previous) {
		IsOptimized = false;
		DependencyGraph dependencyGraph = new DependencyGraph(gridIndexer);
		dependencyGraph.setPartNum(node);
		if (pois.size() == 0) {
			SpatialObject t = new SpatialObject();
			t.setScore(0);
			List<SpatialObject> topk = new ArrayList<SpatialObject>();
			topk.add(t);
			return new OneStepResult((int) (dependencyGraph.safeRegionCnt()), (int) (unsafeCNT), (int) (dependencyGraph.partition()), (int) (dependencyGraph.cornerBLat()), dependencyGraph.getFinalResult());
		}
		topKIndex=new HashMap<>();
		for (SpatialObject spatialObject: previous ) {
			topKIndex.put((gridIndexer.getCellIndex(spatialObject.getGeometry().getCoordinates()[1].x
					, spatialObject.getGeometry().getCoordinates()[1].y)._1().toString()+":"+gridIndexer.getCellIndex(spatialObject.getGeometry().getCoordinates()[1].x
					, spatialObject.getGeometry().getCoordinates()[1].y)._2().toString()),spatialObject);
		}
		HashMap<String, POI> temp = new HashMap<>();
		for (POI poi : pois) {
			double x = poi.getPoint().getX();
			double y = poi.getPoint().getY();
			if (temp.containsValue(x + ":" + y)) {
				temp.get(x + ":" + y).increaseScore();
			} else
				temp.put(x + ":" + y, poi);
			pois = new ArrayList<>();
			pois.addAll(temp.values());
		}
		geometryFactory = new GeometryFactory(new PrecisionModel(), pois.get(0).getPoint().getSRID());
		Grid grid = new Grid(pois, eps);
		PriorityQueue<Block> queue = initQueue(grid, scoreFunction, eps);
		Block block;

		while (dependencyGraph.safeRegionCnt() < k && !queue.isEmpty()) {
			block = queue.poll();
			processBlock(block, eps, scoreFunction, queue, dependencyGraph);
		}
		return new OneStepResult((int) (dependencyGraph.safeRegionCnt()), (int) (unsafeCNT), (int) (dependencyGraph.partition()), (int) (dependencyGraph.cornerBLat()), dependencyGraph.getFinalResult());
	}

	public OneStepResult findBestCatchmentAreas(List<POI> pois, SpatialObject left, SpatialObject up, SpatialObject Corner
			, int node, double eps, int k, ScoreFunctionTotalScore<POI> scoreFunction) {
		IsOptimized = true;
		if (pois.size() == 0) {
			SpatialObject t = new SpatialObject();
			t.setScore(0);
			List<SpatialObject> topk = new ArrayList<SpatialObject>();
			topk.add(t);
			return null;
		}
		geometryFactory = new GeometryFactory(new PrecisionModel(), pois.get(0).getPoint().getSRID());
		this.corner = Corner;
		this.left = left;
		this.up = up;
		this.node = node;
		Grid grid = new Grid(pois, eps);
		PriorityQueue<Block> queue = initQueue(grid, scoreFunction, eps);
		grid = null;
		Block block;
		DependencyGraph dependencyGraph = new DependencyGraph(gridIndexer);
//		System.out.println(left.getScore()+"   "+up.getScore()+"     "+corner.getScore());
		while (dependencyGraph.safeRegionCnt() < k && !queue.isEmpty()) {
			block = queue.poll();
			processBlock(block, eps, scoreFunction, queue, dependencyGraph);
		}
	//	System.out.println("Whole poisCNT in node:" + pois.size() + "      safeCnt:" + dependencyGraph.safeRegionCnt() + "         overallCNT:" + overall + "      optimizedDone:" + opt1 + "      unsafeCNT:" + unsafeCNT);
		return new OneStepResult((int) (dependencyGraph.safeRegionCnt()), (int) (unsafeCNT), (int) (dependencyGraph.partition()), (int) (dependencyGraph.cornerBLat()), dependencyGraph.getFinalResult());
	}

	public Object findBestCatchmentAreas(List<POI> pois, List<BorderResult> border, int node, double eps, int k, ScoreFunctionTotalScore<POI> scoreFunction) {
		DependencyGraph dependencyGraph = new DependencyGraph(gridIndexer);
		if (pois.size() == 0) {
			return dependencyGraph.getFinalResult();
		}
		geometryFactory = new GeometryFactory(new PrecisionModel(), pois.get(0).getPoint().getSRID());
		this.node = node;
		this.border = new HashMap<>();
		//	for (BorderResult b : border)
		//		this.border.put(b.makeKey(), b.score);
		IsOptimized = true;
		Grid grid = new Grid(pois, eps);
		PriorityQueue<Block> queue = initQueue(grid, scoreFunction, eps);
		Block block;

		while (dependencyGraph.safeRegionCnt() < k && !queue.isEmpty()) {
			block = queue.poll();
			processBlock(block, eps, scoreFunction, queue, dependencyGraph);
		}
		//System.out.println("Whole poisCNT in node:" + pois.size() + "      safeCnt:" + dependencyGraph.safeRegionCnt() + "         overallCNT:" + overall + "      optimizedDone:" + opt1 + "      unsafeCNT:" + unsafeCNT);
		return dependencyGraph.getFinalResult().toList();
	}

	private PriorityQueue<Block> initQueue(Grid grid, ScoreFunctionTotalScore<POI> scoreFunction, double eps) {

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

				//	block = new Block(cellPois, scoreFunction, Block.BLOCK_TYPE_CELL, Block.BLOCK_ORIENTATION_VERTICAL,
				//			Block.EXPAND_NONE, eps, geometryFactory);
				if (cellPois.size() > 0) {
					block = new Block(cellPois, scoreFunction, Block.BLOCK_TYPE_CELL, Block.BLOCK_ORIENTATION_VERTICAL,
							Block.EXPAND_NONE, eps, geometryFactory, 0, cellPois.size() - 1);

					queue.add(block);
				}
			}
		}
		return queue;
	}

	private void processBlock(Block block, double eps, ScoreFunction<POI> scoreFunction, PriorityQueue<Block> queue,
							  DependencyGraph dependencyGraph) {
		if (block.type == Block.BLOCK_TYPE_REGION) {
			if (IsOptimized)
				inspectResultOpt(block, eps, dependencyGraph);
			else
				inspectResult(block, eps, dependencyGraph);
		} else {
			List<Block> newBlocks = block.sweep();
			queue.addAll(newBlocks);
		}

		// insert the two derived sub-blocks in the queue
		//if ((block.type == Block.BLOCK_TYPE_SLAB || block.type == Block.BLOCK_TYPE_REGION) && block.pois.size() > 1) {
		if ((block.type == Block.BLOCK_TYPE_SLAB || block.type == Block.BLOCK_TYPE_REGION) && (block.end - block.start + 1) > 1) {
			Block[] derivedBlocks = block.getSubBlocks();
			for (int i = 0; i < derivedBlocks.length; i++) {
				queue.add(derivedBlocks[i]);
			}
		}
		// }
	}

	private void inspectResultOpt(Block block, double eps, DependencyGraph dependencyGraph) {
		// generate candidate result
		overall++;
		int g = gridIndexer.gridSizePerCell();
		Envelope e = geometryFactory.createPoint(block.envelope.centre()).getEnvelopeInternal();
		e.expandBy(eps / 2); // with fixed size eps
		SpatialObject candidate = new SpatialObject(block.envelope.centre().x + ":" + block.envelope.centre().y, block.utilityScore, geometryFactory.toGeometry(e));
		if (duplicate.contains(block.envelope.centre().x + ":" + block.envelope.centre().y + ":" + block.type)) {
			block.type = Block.EXPAND_NONE;
			return;
		} else
			duplicate.add(block.envelope.centre().x + ":" + block.envelope.centre().y + ":" + block.type);
		Tuple2 index = gridIndexer.getCellIndexInGrid(node, candidate);
		int cellInI = (int) index._1();
		int cellInJ = (int) index._2();
		int con = dependencyGraph.overlapCon(candidate);
		boolean opt = false;
		if (cellInI == 0)
			if (candidate.getScore() > border.getOrDefault(new Tuple2(-1, cellInJ - 1), 0.0)
					&& candidate.getScore() > border.getOrDefault(new Tuple2(-1, cellInJ), 0.0)
					&& candidate.getScore() > border.getOrDefault(new Tuple2(-1, cellInJ + 1), 0.0))
				opt = true;
			else opt = false;
		if (cellInI == g - 1)
			if (candidate.getScore() > border.getOrDefault(new Tuple2(g, cellInJ - 1), 0.0)
					&& candidate.getScore() > border.getOrDefault(new Tuple2(g, cellInJ), 0.0)
					&& candidate.getScore() > border.getOrDefault(new Tuple2(g, cellInJ + 1), 0.0))
				opt = true;
			else opt = false;
		if (cellInJ == 0)
			if (candidate.getScore() > border.getOrDefault(new Tuple2(cellInI - 1, -1), 0.0)
					&& candidate.getScore() > border.getOrDefault(new Tuple2(cellInI, -1), 0.0)
					&& candidate.getScore() > border.getOrDefault(new Tuple2(cellInI + 1, -1), 0.0))
				opt = true;
			else opt = false;
		if (cellInJ == g - 1)
			if (candidate.getScore() > border.getOrDefault(new Tuple2(cellInI - 1, g), 0.0)
					&& candidate.getScore() > border.getOrDefault(new Tuple2(cellInI, g), 0.0)
					&& candidate.getScore() > border.getOrDefault(new Tuple2(cellInI + 1, g), 0.0))
				opt = true;
			else opt = false;
		if (con == 0 && !dependencyGraph.IsBorderRegion(candidate))
			dependencyGraph.addSafeRegion(candidate);
			//2nd Condition
		else if (con == 1) {
			// It is not added to dependency graph
			return;
		}
		//3rd Condition
		else if (opt) {
			dependencyGraph.addSafeRegion(candidate);
			opt1++;
		} else if (con == 2 || dependencyGraph.IsBorderRegion(candidate)) {
			unsafeCNT++;
			dependencyGraph.addUnsafeRegion(candidate);
			if (dependencyGraph.IsDependencyIncluded(candidate)) {
				// Do not add Safe
				// Do not add M
				return;
			} else {
				dependencyGraph.increaseSafeCNT();
				dependencyGraph.addM(candidate);
			}

		}
	}

	private void inspectResult(Block block, double eps, DependencyGraph dependencyGraph) {
		// generate candidate result
		Envelope e = geometryFactory.createPoint(block.envelope.centre()).getEnvelopeInternal();
		e.expandBy(eps / 2); // with fixed size eps
		SpatialObject candidate = new SpatialObject();
		try {
			candidate = new SpatialObject(block.envelope.centre().x + ":" + block.envelope.centre().y, block.utilityScore, geometryFactory.toGeometry(e));
		} catch (Exception o) {
			System.out.println(block);
		}
		if (duplicate.contains(block.envelope.centre().x + ":" + block.envelope.centre().y + ":" + block.type)) {
			block.type = Block.EXPAND_NONE;
			return;
		} else
			duplicate.add(block.envelope.centre().x + ":" + block.envelope.centre().y + ":" + block.type);
		boolean isDistinct = true;
		int cellI = Integer.parseInt(gridIndexer.getCellIndex(candidate.getGeometry().getCoordinates()[1].x
				, candidate.getGeometry().getCoordinates()[1].y)._1().toString());
		int cellJ = Integer.parseInt(gridIndexer.getCellIndex(candidate.getGeometry().getCoordinates()[1].x
				, candidate.getGeometry().getCoordinates()[1].y)._2().toString());
		for (int i = -1; i <= 1; i++)
			for (int j = -1; j <= 1; j++) {
				SpatialObject t = topKIndex.getOrDefault(((cellI + i) + ":" + (cellJ + j)), null);
				if (t != null && Generic.intersects(candidate, t))
					isDistinct = false;
			}
		if (isDistinct) {
			int con = dependencyGraph.overlapCon(candidate);

			//1st Condition
			if (con == 0 && !dependencyGraph.IsBorderRegion(candidate))
				dependencyGraph.addSafeRegion(candidate);
				//2nd Condition
			else if (con == 1)
				// It is not added to dependency graph
				return;
				//3rd Condition
			else if (con == 2 || dependencyGraph.IsBorderRegion(candidate)) {
				unsafeCNT++;
				dependencyGraph.addUnsafeRegion(candidate);
				if (dependencyGraph.IsDependencyIncluded(candidate)) {
					// Do not add Safe
					// Do not add M
					return;
				} else {
					dependencyGraph.increaseSafeCNT();
					dependencyGraph.addM(candidate);
				}

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
		return (int) (n * resolution);
	}
}
