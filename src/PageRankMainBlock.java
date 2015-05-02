
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;



public class PageRankMainBlock {
	public static void main(String[] args) throws Exception {
		Context c = new Context();
		BlockMapperPass0 pmr = new BlockMapperPass0();
		BlockReducerPass0 prr = new BlockReducerPass0();
		BufferedReader br = new BufferedReader(new FileReader("edges_very_small.txt"));
		String line = br.readLine();
		while(line != null){
			pmr.mapper(new LongWritable(0), new Text(line), c);
			line = br.readLine();
		}
		br.close();
		for (Entry<LongWritable, ArrayList<Text>> m : c.mapData.entrySet())
			prr.reduce(m.getKey(), m.getValue(), c);
		int round = 0;
		double resid = 1.;
		while (resid > .01){
			PageRankBlockMapper pmrN = new PageRankBlockMapper();
			PageRankBlockReducer prrN = new PageRankBlockReducer();
			Context c2 = new Context();
			c2.findCounter(PageRankEnum.TOTAL_NODES).setValue(c.findCounter(PageRankEnum.TOTAL_NODES).getValue());
			c2.findCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).setValue(0);
			for (Entry<LongWritable, Text> r : c.reduceData)
				pmrN.mapper(r.getKey(), r.getValue(), c2);
			
			for (Entry<LongWritable, ArrayList<Text>> m : c2.mapData.entrySet())
				prrN.reduce(m.getKey(), m.getValue(), c2);
			c = c2;
			round++;
			System.out.println("sum: " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
			System.out.println(" avg " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/(CONST.SIG_FIG_FOR_DOUBLE_TO_LONG * c.getCounter(PageRankEnum.TOTAL_NODES).getValue()));
			System.out.println("0: " + c.reduceData.get(0).getKey().get() + " " + c.reduceData.get(0).getValue());
			resid = c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;
		}
		System.out.println("outer rounds: " + round);

		
	}
}

