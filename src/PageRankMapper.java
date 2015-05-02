

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper {

	public void mapper(LongWritable keyin, Text val, Context context){
		String[] info = val.toString().split("\t");
		String[] toList = info[0].split(",");
		Double pr = Double.parseDouble(info[1]);
		if (toList.length == 0 || (toList.length == 1 && toList[0].equals(""))){
			context.findCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment(
					(long)(pr * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5)
					);
		} else {
			for (String to : toList){
				int toID = Integer.parseInt(to);
				context.write(new LongWritable(toID), new Text(Double.toString(pr/toList.length)));
						
			}
		}
		context.write(keyin, new Text(info[0] + "\t" + Double.toString(pr)));
			
		
	}
}
