package minMaxMedian;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.io.*;

public class myNumberKey implements WritableComparable<myNumberKey>{
	IntWritable naturalKey=new IntWritable();
	IntWritable sortingKey=new IntWritable();
	public myNumberKey()
	{
	}
	public myNumberKey(IntWritable n)
	{
		this.sortingKey=n;
		this.naturalKey.set(1);
	}
	@Override
	public int compareTo(myNumberKey toBeComparedKey)
	{
		if(this.sortingKey.get()==toBeComparedKey.sortingKey.get())
			return 0;
		else if(this.sortingKey.get()<toBeComparedKey.sortingKey.get())
			return -1;
		else
			return 1;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.naturalKey.write(out);
		this.sortingKey.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	this.naturalKey.readFields(in);
	this.sortingKey.readFields(in);
}

}
