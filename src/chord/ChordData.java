package chord;

public class ChordData
{
	private byte[] hash;
	private byte[] data;
	
	public ChordData(byte[] hash, byte[] data)
	{
		this.hash = hash;
		this.data = data;
	}
	
	public byte[] getHash()
	{
		return hash;
	}
	
	public byte[] getData()
	{
		return data;
	}
	
	public void setData(byte[] data)
	{
		this.data = data;
	}
}