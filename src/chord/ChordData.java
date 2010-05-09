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

	public String getHashString()
	{
		String hashString = "";

		for(int b = 0; b < hash.length; b++)
		{
			int value = (int)hash[b] & 0xFF;

			if(value < 0x10)
			{
				hashString += "0";
			}

			hashString += Integer.toHexString(value);
		}

		return hashString.toUpperCase();
	}

	public String toString()
	{
		return getHashString() + " -> " + new String(data);
	}
}