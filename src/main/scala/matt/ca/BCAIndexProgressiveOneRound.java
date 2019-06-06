package matt.ca;
import matt.*;
import scala.Array;
import scala.Int;
import scala.collection.JavaConversions;

import matt.definitions.GridIndexer;
import matt.score.ScoreFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import java.util.*;

import scala.collection.JavaConverters;
import shapeless.Tuple;

public class BCAIndexProgressiveOneRound  {
	private boolean IsOptimized;
	private boolean distinctMode;
	private GeometryFactory geometryFactory;
	private long overallStartTime, resultEndTime;
	private GridIndexer gridIndexer;
	private int opt1=0;
	private int overall=0;
	private int overlapUnsafe=0;
	private int overlapSafe=0;
	private int overlapBorder=0;
	private int[][] up,left,corner;
	private int node;
	public BCAIndexProgressiveOneRound(boolean distinctMode,GridIndexer gridIndexer) {
		super();
		this.distinctMode = distinctMode;
		this.gridIndexer=gridIndexer;
	}


	public Object findBestCatchmentAreas(List<POI> pois, double eps, int k,
			ScoreFunction<POI> scoreFunction) {
		geometryFactory = new GeometryFactory(new PrecisionModel(), pois.get(0).getPoint().getSRID());
		Grid grid = new Grid(pois, eps);
		PriorityQueue<Block> queue = initQueue(grid, scoreFunction, eps);
		Block block;
		DependencyGraph dependencyGraph=new DependencyGraph(gridIndexer);
		while (dependencyGraph.safeRegionCnt() < k && !queue.isEmpty()) {
			block = queue.poll();
			processBlock(block, eps, scoreFunction, queue, dependencyGraph);
		}
		return dependencyGraph.getFinalResult().toList();
	}

	public Object findBestCatchmentAreas(List<POI> pois,int nodeNum,  int[][] up,int[][] left,int[][] corner
			, double eps, int k, ScoreFunction<POI> scoreFunction) {
		this.node=nodeNum;
		geometryFactory = new GeometryFactory(new PrecisionModel(), pois.get(0).getPoint().getSRID());
		this.up = up;
		this.left = left;
		this.corner = corner;
		IsOptimized = true;
		Grid grid = new Grid(pois, eps);
		PriorityQueue<Block> queue = initQueue(grid, scoreFunction, eps);
		Block block;
		DependencyGraph dependencyGraph = new DependencyGraph(gridIndexer);
		while (dependencyGraph.safeRegionCnt() < k && !queue.isEmpty()) {
			block = queue.poll();
			processBlock(block, eps, scoreFunction, queue, dependencyGraph);
		}
		System.out.println(pois.size()+"      "+dependencyGraph.safeRegionCnt()+"         "+overall + "      " + opt1 + "      " + overlapUnsafe + "      " + overlapBorder + "      " + overlapSafe + "     " + dependencyGraph.indexToSpatialObjUnsafe().values().size());
		return dependencyGraph.getFinalResult().toList();
	}

	private PriorityQueue<Block> initQueue(Grid grid, ScoreFunction<POI> scoreFunction, double eps) {

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
			DependencyGraph dependencyGraph) {

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
			if (IsOptimized)
				inspectResultOpt(block, eps, dependencyGraph);
			else
				inspectResult(block, eps, dependencyGraph);
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

	private void inspectResultOpt(Block block, double eps,DependencyGraph dependencyGraph) {
		// generate candidate result
		overall++;
		Envelope e = geometryFactory.createPoint(block.envelope.centre()).getEnvelopeInternal();
		e.expandBy(eps / 2); // with fixed size eps
		SpatialObject candidate = new SpatialObject(block.envelope.centre().x + ":" + block.envelope.centre().y, null,
				null, block.utilityScore, geometryFactory.toGeometry(e));

		scala.Tuple2 index = gridIndexer.getCellIndexInGrid(node, candidate);
		int cellI = (int) index._1();
		int cellJ = (int) index._2();
		if (!dependencyGraph.IsOverlapAnyRegion(candidate) && !dependencyGraph.IsBorderRegion(candidate))
			dependencyGraph.addSafeRegion(candidate);
			//2nd Condition
		else if (dependencyGraph.IsOverlapSafeRegion(candidate)) {
			overlapSafe++;
			// It is not added to dependency graph
			return;
		}
		//3rd Condition
		else if ((((cellI==0)&&(cellJ==0)) && candidate.getScore() > corner[0][0] + corner[0][1] + corner[1][0] + corner[1][1])
				|| ((cellJ==0&&cellI>0)&& candidate.getScore() > up[0][cellI] + up[0][cellI - 1] + up[1][cellI] + up[1][cellI - 1])
				|| ((cellI==0&&cellJ>0) && candidate.getScore() > left[cellJ][0] + left[cellJ - 1][0] + left[cellJ][1] + left[cellJ - 1][1])) {
			dependencyGraph.addSafeRegion(candidate);
			opt1++;
		} else if (dependencyGraph.IsOverlapUnsafeRegion(candidate) || dependencyGraph.IsBorderRegion(candidate)) {
			if (dependencyGraph.IsOverlapUnsafeRegion(candidate))
				overlapUnsafe++;
			if (dependencyGraph.IsBorderRegion(candidate))
				overlapBorder++;
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

	private void inspectResult(Block block, double eps,DependencyGraph dependencyGraph) {
		// generate candidate result
		Envelope e = geometryFactory.createPoint(block.envelope.centre()).getEnvelopeInternal();
		e.expandBy(eps / 2); // with fixed size eps
		SpatialObject candidate = new SpatialObject(block.envelope.centre().x + ":" + block.envelope.centre().y, null,
				null, block.utilityScore, geometryFactory.toGeometry(e));

		//1st Condition
		if (!dependencyGraph.IsOverlapAnyRegion(candidate) && !dependencyGraph.IsBorderRegion(candidate))
			dependencyGraph.addSafeRegion(candidate);
			//2nd Condition
		else if (dependencyGraph.IsOverlapSafeRegion(candidate))
			// It is not added to dependency graph
			return;
			//3rd Condition
		else if (dependencyGraph.IsOverlapUnsafeRegion(candidate) || dependencyGraph.IsBorderRegion(candidate)) {
			dependencyGraph.addUnsafeRegion(candidate);
			if(dependencyGraph.IsDependencyIncluded(candidate)){
				// Do not add Safe
				// Do not add M
				return;
			}
			else{
				dependencyGraph.increaseSafeCNT();
				dependencyGraph.addM(candidate);
			}

		}

/*		if (isDistinct) {

			result.setAttributes(new HashMap<Object, Object>());
			result.getAttributes().put("coveredPoints", block.pois);

			resultEndTime = (System.nanoTime() - overallStartTime) / 1000000;
			result.getAttributes().put("executionTime", resultEndTime);

			System.out.println("Results so far: " + topk.size());
		}*/
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