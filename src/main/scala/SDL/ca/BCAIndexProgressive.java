package SDL.ca;

import SDL.Grid;
import SDL.POI;
import SDL.SpatialObject;
import SDL.definitions.GridIndexer;
import SDL.score.ScoreFunction;
import SDL.score.ScoreFunctionTotalScore;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import java.util.*;

public class BCAIndexProgressive extends BCAFinder<POI> {

    private HashSet<String> duplicate = new HashSet<>();
    private boolean distinctMode;
    private GeometryFactory geometryFactory;

    public BCAIndexProgressive(boolean distinctMode, GridIndexer gridIndexer) {
        super();
        this.distinctMode = distinctMode;
    }

    @Override
    public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k, ScoreFunction<POI> scoreFunction) {
        return findBestCatchmentAreas(pois, eps, k, (ScoreFunctionTotalScore) scoreFunction, new ArrayList<>(), -1);
    }

    public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k, ScoreFunctionTotalScore<POI> scoreFunction) {
        return findBestCatchmentAreas(pois, eps, k, scoreFunction, new ArrayList<>(), -1);
    }

    public List<SpatialObject> findBestCatchmentAreas(List<POI> pois, double eps, int k,
                                                      ScoreFunctionTotalScore<POI> scoreFunction, List<SpatialObject> previous, int node) {

        HashMap<String, POI> temp = new HashMap<>();
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
                              List<SpatialObject> topk, List<SpatialObject> previous) {
        try {
            if (block.type == Block.BLOCK_TYPE_REGION) {
                inspectResult(block, eps, topk, previous);
            } else {
                if (block != null && block.orderedRectangles.size() > 0) {

                    List<Block> newBlocks = block.sweep();
                    queue.addAll(newBlocks);

                }
            }
            if ((block.type == Block.BLOCK_TYPE_SLAB || block.type == Block.BLOCK_TYPE_REGION) && block.start < block.end) {
                Block[] derivedBlocks = block.getSubBlocks();
                for (int i = 0; i < derivedBlocks.length; i++) {
                    queue.add(derivedBlocks[i]);
                }
            }
        } catch (OutOfMemoryError e) {
            System.err.println("current block location:   " + block.envelope.centre().getX() + ":" + block.envelope.centre().getY());
            System.err.println("current block size:   " + (block.end - block.start + 1));
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
        if (duplicate.contains(myRound(block.envelope.centre().x, 10000) + ":" + myRound(block.envelope.centre().y, 10000) + ":" + block.type)) {
            block.type = Block.EXPAND_NONE;
            return;
        } else
            duplicate.add(myRound(block.envelope.centre().x, 10000) + ":" + myRound(block.envelope.centre().y, 10000) + ":" + block.type);
        SpatialObject result = new SpatialObject(block.envelope.centre().y + "," + block.envelope.centre().x, block.relevanceScore, geometryFactory.toGeometry(e));
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
        //	isDistinct=true;
        if (isDistinct) {
            if (result.getGeometry() != null) {
                topk.add(result);
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