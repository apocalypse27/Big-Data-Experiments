package matrixMult;

public class cell {
	int i;
	int j;
	int cellValue;
	
	public cell(int i,int j, int value)
	{
		this.i=i;
		this.j=j;
		this.cellValue=value;
	}
	public cell()
	{
		this.i=-1;
		this.j=-1;
		this.cellValue=Integer.MIN_VALUE;
	}

}
