import java.io.IOException;

import org.apache.hadoop.io.*;

public class PageRankMapperZero {
	
	
	public void mapper(LongWritable keyin, Text val, Context context){
		String[] info = val.toString().split(" ");
		try {
			double select = Double.parseDouble(info[0]);
			LongWritable fromInt = new LongWritable(Integer.parseInt(info[1]));
			LongWritable toInt = new LongWritable(Integer.parseInt(info[2]));
			Text toText = new Text(info[2]);
			Text nullTo = new Text("-1");
			if (Util.retainEdgeByNodeID(select)){
				context.write(fromInt, toText);
			} else {
				context.write(fromInt, nullTo);
			}
			context.write(toInt, nullTo);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
