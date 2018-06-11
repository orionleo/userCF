package userCF_Step3;
import userCF_Tools.*;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class userCF_Step3_Mapper extends Mapper<LongWritable, Text, CompositeKey, Text> {

	private CompositeKey outputKey = new CompositeKey();
	private Text outputValue = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String userID = value.toString().split("\t")[0];
		String[] itemID_score_Array = value.toString().split("\t")[1].split(",");
		
		for(String itemID_score : itemID_score_Array) {
			
			String itemID = itemID_score.split("_")[0];
			String score = itemID_score.split("_")[1];
			
			outputKey.setMemberKey1(itemID);
			outputKey.setMemberKey2(userID);
			outputValue.set(userID + "_" + score);
			context.write(outputKey, outputValue);
		}
	}
}
