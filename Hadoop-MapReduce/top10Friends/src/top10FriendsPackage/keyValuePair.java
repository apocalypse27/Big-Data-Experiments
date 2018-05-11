package top10FriendsPackage;

import org.apache.commons.lang3.StringUtils;

public class keyValuePair implements Comparable<keyValuePair>{
	String key;
	int Countvalue;
	
	public keyValuePair(String key,int value)
	{
		this.key=key;
		this.Countvalue=value;
	}
	@Override
	public int compareTo(keyValuePair pair)
	{
		if(this.Countvalue<pair.Countvalue)
		{
			return -1;
		}
		else if(this.Countvalue==pair.Countvalue)
			return 0;
		else
		{
			return 1;
		}
	}
	
	@Override
	public String toString()
	{
		return(this.key + "\t" + this.Countvalue);
	}
}