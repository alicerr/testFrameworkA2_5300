import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class Context {
	public HashMap<PageRankEnum, Counter> counters = new HashMap<PageRankEnum, Counter>();
	public HashMap<LongWritable, ArrayList<Text>> mapData = new HashMap<LongWritable, ArrayList<Text>>();
	public ArrayList<Entry<LongWritable, Text>> reduceData = new ArrayList<Entry<LongWritable, Text>>();
	public Counter getCounter(PageRankEnum e){
		return counters.get(e);
	}
	public Counter findCounter(PageRankEnum e){
		if (counters.containsKey(e))
			return counters.get(e);
		else{
			Counter c = new Counter();
			counters.put(e, c);
			return c;
		}
	}
	public void write(LongWritable key, Text val){
		String callingClass = KDebug.getCallerCallerClassName();
		if (callingClass.contains("apper")){
			if (mapData.containsKey(key)){
				mapData.get(key).add(val);
			} else {
				ArrayList<Text> t = new ArrayList<Text>();
				t.add(val);
				mapData.put(key, t);
			}
		} else {
			reduceData.add(new AbstractMap.SimpleEntry<LongWritable, Text>(key, val));
		}
	}

	
}
