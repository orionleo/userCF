package userCF_Step5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class userCF_Step5_Mapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	
	private CompositeKey outputKey = new CompositeKey();
	private Text outputValue = new Text();
	
	List<String> CacheList = new ArrayList<String>();
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		
		FSDataInputStream myInputStream = fs.open(new Path("/userCF/Step1_Output/part-r-00000"));
		BufferedReader br = new BufferedReader(new InputStreamReader(myInputStream));
		
		String line = null;
		while((line = br.readLine()) != null) {		
			CacheList.add(line);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// A	1_2.90,2_2.15,3_10.94,4_3.93,5_0.75,6_1.23
		String userID_matrix1 = value.toString().split("\t")[0];
		String[] itemID_score_matrix1 = value.toString().split("\t")[1].split(",");
		
		for(String line : CacheList) {
			
			// A	1_1,2_0,3_5,4_3,5_0,6_0
			String userID_matrix2 = line.split("\t")[0];
			
			if(userID_matrix1.equals(userID_matrix2)) {
				
				String[] itemID_score_matrix2 = line.split("\t")[1].split(",");
				
				for(int i = 0; i < 6; i++) {
					
					String score_matrix2 = itemID_score_matrix2[i].split("_")[1];
					String itemID_matrix1 = itemID_score_matrix1[i].split("_")[0];
					
					outputKey.setMemberKey1(userID_matrix1);
					outputKey.setMemberKey2(itemID_matrix1);
					
					if(Integer.valueOf(score_matrix2) != 0) {
						
						outputValue.set(itemID_matrix1 + "_" + "0.00");
						context.write(outputKey, outputValue);
					}
					else {
						
						String score_matrix1 = itemID_score_matrix1[i].split("_")[1];
						outputValue.set(itemID_matrix1 + "_" + score_matrix1);
						context.write(outputKey, outputValue);
					}
				}
			}
		}
	}
}
