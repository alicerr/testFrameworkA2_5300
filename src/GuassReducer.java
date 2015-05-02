import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class GuassReducer  {
	public void reduce(LongWritable key, Iterable<Text> vals, Context context){

		//information holders for vals
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		HashMap<Integer, ArrayList<Edge>> innerEdges = new HashMap<Integer, ArrayList<Edge>>();
		HashMap<Integer, Double> outerEdges = new HashMap<Integer, Double>();
		double inBlockSink = 0.;
		String outerEdgesString = "";
		String innerEdgesString = "";
		
		//get values passed into function
		for (Text val : vals){
			String[] info = val.toString().split(CONST.L0_DIV, -1);
			byte marker = Byte.parseByte(info[CONST.MARKER_INDEX_L0]);
			
			//block data
			if (marker == CONST.ENTIRE_BLOCK_DATA_MARKER){
				inBlockSink = Util.fillMapsFromBlockString(info, nodes, innerEdges, null);
				outerEdgesString = info[CONST.OUTER_EDGE_LIST];
				innerEdgesString = info[CONST.INNER_EDGE_LIST];
				
			} 
			//incoming edge data
			else if (marker == CONST.INCOMING_EDGE_MARKER){
				OuterEdgeValue incoming = new OuterEdgeValue(info);
				if (outerEdges.containsKey(incoming.to)){
					outerEdges.put(incoming.to, outerEdges.get(incoming.to) + incoming.pr);
				} else {
					outerEdges.put(incoming.to, incoming.pr);
				}
			}
		}
		
		//general variables from counters and constants
		double outOfBlockSink = CONST.DAMPING_FACTOR*((context.findCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue() + .5)/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG - inBlockSink);
		double totalNodes = context.findCounter(PageRankEnum.TOTAL_NODES).getValue();
		double basePageAddition = CONST.RANDOM_SURFER * CONST.BASE_PAGE_RANK + outOfBlockSink/totalNodes;
		double oldInBlockSink = inBlockSink;
		//per round holders
		HashMap<Integer, Node> nodesLastPass = new HashMap<Integer, Node>();
		HashMap<Integer, Node> nodesThisPass = new HashMap<Integer, Node>();
		boolean converged = false;
		double residualSum = 0.;
		double newInBlockSink = 0.;
		for (Node n : nodes.values()){
			nodesLastPass.put(n.id, new Node(n));
		}
		int round = 0;
		//run convergence
		while (!converged){
			double baseInBlockPageAddition = CONST.DAMPING_FACTOR * (inBlockSink/(double)totalNodes);
			for (Node n : nodesLastPass.values()){
				
				//base pr
				double pr = basePageAddition + baseInBlockPageAddition;
				
				//incoming pr
				if (outerEdges.containsKey(n.id))
					pr += CONST.DAMPING_FACTOR * outerEdges.get(n.id);
				
				//in block pr
				if (innerEdges.containsKey(n.id))
					for (Edge e : innerEdges.get(n.id))
						if (nodesThisPass.containsKey(e.from))
							pr += CONST.DAMPING_FACTOR * nodesThisPass.get(e.from).prOnEdge();
						else
							pr += CONST.DAMPING_FACTOR * nodesLastPass.get(e.from).prOnEdge();
				
				//residual
				double residual = Math.abs((pr - n.getPR()))/pr;
				residualSum += residual;
				
				//save value
				Node nPrime = new Node(n);
				nPrime.setPR(pr);
				nodesThisPass.put(nPrime.id, nPrime);
				
				//look for sink
				if (nPrime.edges() == 0)
					newInBlockSink += pr;
				
			}
			//reset holders
			nodesLastPass = nodesThisPass;
			nodesThisPass = new HashMap<Integer, Node>();
			inBlockSink = newInBlockSink;
			newInBlockSink = 0;
			
			//check for convergence
			converged = residualSum < CONST.RESIDUAL_SUM_DELTA;
			//System.out.println(key + " " + residualSum);
			residualSum = 0;
			round++;
			
		}
		System.out.println("reducer rounds: " + round);
		//get residual from values passed into reducer
		double residualSumOuter = 0.;
		for (Node n : nodesLastPass.values()){
			double residual = Math.abs((n.getPR() - nodes.get(n.id).getPR()))/n.getPR();
			residualSumOuter += residual;
		}
		context.findCounter(PageRankEnum.RESIDUAL_SUM).increment((long) (residualSumOuter * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG));
		context.findCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment((long) ((oldInBlockSink - inBlockSink)*CONST.SIG_FIG_FOR_DOUBLE_TO_LONG));
		//save updated block
		String block = Util.getBlockDataAsString(nodesLastPass, innerEdgesString, outerEdgesString);
		context.write(key, new Text(block));
	}
	

}
