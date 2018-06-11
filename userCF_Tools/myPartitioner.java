package userCF_Tools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class myPartitioner extends Partitioner<CompositeKey, Text> {

	@Override
	public int getPartition(CompositeKey comKey, Text value, int NumOfPartitions) {
		return Math.abs(comKey.getMemberKey1().hashCode() % NumOfPartitions);
	}
}
