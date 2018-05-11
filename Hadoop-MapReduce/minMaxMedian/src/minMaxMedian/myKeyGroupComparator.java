package minMaxMedian;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
public class myKeyGroupComparator extends WritableComparator {
    public myKeyGroupComparator() {
        super(myNumberKey.class, true);
    }
    @Override
    public int compare(WritableComparable k1, WritableComparable k2) {
    	myNumberKey key1 = (myNumberKey) k1;
    	myNumberKey key2 = (myNumberKey) k2;
        return key1.naturalKey.compareTo(key2.naturalKey);
    }
}
