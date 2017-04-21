package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	Text outKey = new Text();
	IntWritable outValue = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] words = value.toString().split(",");
		int surv = Integer.parseInt(words[1]);
		if(surv==0){
			outKey.set(words[4]);
			context.write(outKey, outValue);
		}
	}
}
