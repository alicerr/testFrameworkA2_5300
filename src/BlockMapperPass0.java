import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;



public class BlockMapperPass0  {
	
	public void mapper(LongWritable keyin, Text val, Context context){
		
		String[] info = val.toString().split(" ");
		try {
			double select = Double.parseDouble(info[0]);
			int fromInt = Integer.parseInt(info[1]);
			int toInt = Integer.parseInt(info[2]);
			int fromBlock = Util.idToBlock(fromInt);
			int toBlock = Util.idToBlock(toInt);
			
			
			if (Util.retainEdgeByNodeID(select)){
				context.write(new LongWritable(fromBlock), new Text(CONST.SEEN_EDGE_MARKER + CONST.L0_DIV + fromInt + CONST.L0_DIV + toInt));
			} else {
				context.write(new LongWritable(fromBlock), new Text(CONST.SEEN_NODE_MARKER + CONST.L0_DIV + fromInt));
				
			}
			context.write(new LongWritable(toBlock), new Text(CONST.SEEN_NODE_MARKER + CONST.L0_DIV + toInt));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
}