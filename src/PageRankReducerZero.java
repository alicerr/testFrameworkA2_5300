import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;



public class PageRankReducerZero{

	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		String toList = "";
		for (Text val : vals){
			if (!val.toString().equals("-1"))
				toList += "," + val.toString();
		}
		if (toList.length() > 0){
			toList = toList.substring(1);
		} 
		context.write(key, new Text(toList + "\t" + CONST.BASE_PAGE_RANK));
		context.findCounter(PageRankEnum.TOTAL_NODES).increment(1);
	}
}
