package userCF_Step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import userCF_Tools.*;

public class userCF_Step1_Mapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	
	private CompositeKey outputKey = new CompositeKey();
	private Text outputValue = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] Fields = value.toString().split(",");
		String userID = Fields[0];
		String itemID = Fields[1];
		String score = Fields[2];
		
		outputKey.setMemberKey1(userID);
		outputKey.setMemberKey2(itemID);
		outputValue.set(itemID + "_" + score);
		context.write(outputKey, outputValue);
	}
}
