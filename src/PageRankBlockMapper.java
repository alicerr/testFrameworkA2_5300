import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class PageRankBlockMapper  {
	
	public void mapper(LongWritable keyin, Text val, Context context){
		
		//keep block text, we don't need to recreate these
		String[] info = val.toString().split(CONST.L0_DIV, -1);
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		HashMap<Integer, ArrayList<Edge>> outerEdges = new HashMap<Integer, ArrayList<Edge>>();

		double sinks = Util.fillMapsFromBlockString(info, nodes, null, outerEdges);
		

		context.findCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment((long) (sinks * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
		for (ArrayList<Edge> ae : outerEdges.values()){
			for (Edge e : ae){
				context.write(new LongWritable(Util.idToBlock(e.to)), new Text(CONST.INCOMING_EDGE_MARKER + CONST.L0_DIV + e.to + CONST.L0_DIV + nodes.get(e.from).prOnEdge()));
			}
		}
		context.write(keyin, val);
	
	}
}
	

