package userCF_Tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {

	private String memberKey1;
	private String memberKey2;
	
	// Constructor
	public CompositeKey() {}
	public CompositeKey(String memberKey1, String memberKey2) {
		this.memberKey1 = memberKey1;
		this.memberKey2 = memberKey2;
	}

	// Get/Set
	public String getMemberKey1() {
		return memberKey1;
	}
	public void setMemberKey1(String memberKey1) {
		this.memberKey1 = memberKey1;
	}
	public String getMemberKey2() {
		return memberKey2;
	}
	public void setMemberKey2(String memberKey2) {
		this.memberKey2 = memberKey2;
	}

	// Serialization
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(memberKey1);
		out.writeUTF(memberKey2);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.memberKey1 = in.readUTF();
		this.memberKey2 = in.readUTF();
	}

	// compareTo
	@Override
	public int compareTo(CompositeKey other) {
		return this.memberKey1.compareTo(other.memberKey1);
	}
}
