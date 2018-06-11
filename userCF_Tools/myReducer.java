package userCF_Tools;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<CompositeKey, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		StringBuilder sBuilder = new StringBuilder();
		for(Text value : values) {
			sBuilder.append(value + ",");
		}
		
		String str = null;
		if(sBuilder.toString().endsWith(",")) {
			str = sBuilder.substring(0, sBuilder.length() - 1);
		}
		
		outputKey.set(key.getMemberKey1());
		outputValue.set(str);
		context.write(outputKey, outputValue);
	}
}
