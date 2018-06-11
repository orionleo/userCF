package userCF_Step4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import userCF_Tools.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class userCF_Step4_Mapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	
	private CompositeKey outputKey = new CompositeKey();
	private Text outputValue = new Text();
	
	List<String> CacheList = new ArrayList<String>();
	
	private DecimalFormat df = new DecimalFormat("0.00");
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		
		FSDataInputStream myInputStream = fs.open(new Path("/userCF/Step3_Output/part-r-00000"));
		BufferedReader br = new BufferedReader(new InputStreamReader(myInputStream));
		
		String line = null;
		while((line = br.readLine()) != null) {		
			CacheList.add(line);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// A	A_1.00,B_0.00,C_0.08,D_0.15,E_0.93,F_0.43
		String userID_matrix1 = value.toString().split("\t")[0];
		String[] otherUserID_score_matrix1 = value.toString().split("\t")[1].split(",");
		
		for(String line : CacheList) {
			
			// 1	A_1,B_0,C_5,D_10,E_0,F_0
			String itemID_matrix2 = line.split("\t")[0];
			String[] userID_score_matrix2 = line.split("\t")[1].split(",");
			
			Double result = 0.00;
			for(int i = 0; i < 6; i++) {
				
				String score_matrix1 = otherUserID_score_matrix1[i].split("_")[1];
				String score_matrix2 = userID_score_matrix2[i].split("_")[1];
				result += Double.valueOf(score_matrix1) * Double.valueOf(score_matrix2);
			}
			
			outputKey.setMemberKey1(userID_matrix1);
			outputKey.setMemberKey2(itemID_matrix2);
			outputValue.set(itemID_matrix2 + "_" + df.format(result));
			context.write(outputKey, outputValue);
		}
	}
}
