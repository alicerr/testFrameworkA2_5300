

import java.io.IOException;

import org.apache.hadoop.io.*;

public class PageRankReducer  {
	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		double redistributeValue = context.findCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue()/(context.findCounter(PageRankEnum.TOTAL_NODES).getValue() * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
		double newPageRank = CONST.RANDOM_SURFER*CONST.BASE_PAGE_RANK + redistributeValue;
		double oldPageRank = 0.;
		String toList = "";
		for (Text val : vals){
			if (val.toString().contains("\t")){
				String[] info = val.toString().split("\t");
				toList = info[0];
				oldPageRank = Double.parseDouble(info[1]);
			} else {
				newPageRank += CONST.DAMPING_FACTOR * Double.parseDouble(val.toString());
			}
			
		}
		double residualValue = Math.abs(newPageRank - oldPageRank)/newPageRank;
		context.findCounter(PageRankEnum.RESIDUAL_SUM).increment((long)(residualValue * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
		context.write(key, new Text(toList + "\t" + newPageRank + "\t" + residualValue));
		
	}

}
