package userCF_Tools;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class mySortComparator extends WritableComparator {

	public mySortComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		CompositeKey comKey1 = (CompositeKey)wc1;
		CompositeKey comKey2 = (CompositeKey)wc2;
		int CompareValue = comKey1.getMemberKey1().compareTo(comKey2.getMemberKey1());
		if(CompareValue == 0) {
			CompareValue = comKey1.getMemberKey2().compareTo(comKey2.getMemberKey2());
		}
		return CompareValue;
	}
}
