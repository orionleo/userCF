package userCF_Step2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import userCF_Tools.*;

public class userCF_Step2_Mapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	
	private CompositeKey outputKey = new CompositeKey();
	private Text outputValue = new Text();
	
	private DecimalFormat df = new DecimalFormat("0.00");
	
	List<String> cacheList = new ArrayList<String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		
		FSDataInputStream myInputStream = fs.open(new Path("/userCF/Step1_Output/part-r-00000"));
		BufferedReader br = new BufferedReader(new InputStreamReader(myInputStream));
		
		String line = null;
		while((line = br.readLine()) != null) {
			cacheList.add(line);
		}
		
		myInputStream.close();
		br.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String userID_matrix1 = value.toString().split("\t")[0];
		String[] itemID_score_Array_matrix1 = value.toString().split("\t")[1].split(",");
		
		double denominator_matrix1 = 0;
		for(String itemID_score_matrix1 : itemID_score_Array_matrix1) {	
			String score_matrix1 = itemID_score_matrix1.split("_")[1];
			denominator_matrix1 += Integer.valueOf(score_matrix1) * Integer.valueOf(score_matrix1);
		}
		denominator_matrix1 = Math.sqrt(denominator_matrix1);
		
		for(String line : cacheList) {		
			String userID_matrix2 = line.split("\t")[0];
			String[] itemID_score_Array_matrix2 = line.split("\t")[1].split(",");
			
			double denominator_matrix2 = 0;
			for(String itemID_score_matrix2 : itemID_score_Array_matrix2) {		
				String score_matrix2 = itemID_score_matrix2.split("_")[1];
				denominator_matrix2 += Integer.valueOf(score_matrix2) * Integer.valueOf(score_matrix2);	
			}
			denominator_matrix2 = Math.sqrt(denominator_matrix2);
			
			int result = 0;
			for(int i = 0; i < itemID_score_Array_matrix1.length; i++) {
				String score_matrix1 = itemID_score_Array_matrix1[i].split("_")[1];
				String score_matrix2 = itemID_score_Array_matrix2[i].split("_")[1];
				result += Integer.valueOf(score_matrix1) * Integer.valueOf(score_matrix2);
			}
			
			double cos = result / (denominator_matrix1 * denominator_matrix2);
			
			outputKey.setMemberKey1(userID_matrix1);
			outputKey.setMemberKey2(userID_matrix2);
			outputValue.set(userID_matrix2 + "_" + df.format(cos));
			context.write(outputKey, outputValue);
		}
	}
}
