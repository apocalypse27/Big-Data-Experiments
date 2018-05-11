package top10FriendsPackage;
import java.util.*;
public class outputList {
	private final int topK=10;
	ArrayList<keyValuePair> opList=new ArrayList<>();
	
	public outputList(){
		/*Does nothing*/
	}
	
	protected void add(String key,int value)
	{
		if(this.opList.size()<=topK)
		{
			this.opList.add(new keyValuePair(key,value));
		}
		if(this.opList.size()>topK)
		{
			Collections.sort(this.opList);
			if(value>this.opList.get(0).Countvalue)
			{
			    this.opList.set(0,new keyValuePair(key,value));
			}
		}
	}
	protected void clear()
	{
		this.opList.clear();
		
	}
	
	

}
