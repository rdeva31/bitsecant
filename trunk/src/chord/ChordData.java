package chord;

import java.util.*;

public class ChordData
{
	private byte[] hash;
	private byte[] data;

	/**
	 * Initializes an empty chorddata without a hash and data.
	 */
	public ChordData()
	{
		hash = null;
		data = null;
	}

	/**
	 * Creates a ChordData with specified hash and data.  The hash is more of a key
	 * than it is a hash of data.
	 * @param hash key to refer to data
	 * @param data data
	 */
	public ChordData(byte[] hash, byte[] data)
	{
		this.hash = hash;
		this.data = data;
	}

	/**
	 * Returns the hash used to instantiate the data
	 * @return hash or null if not specified
	 */
	public byte[] getHash()
	{
		return hash;
	}

	/**
	 * Data stored by this object
	 * @return data
	 */
	public byte[] getData()
	{
		return data;
	}

	/**
	 * Replaces the data stored in the object with data
	 * @param data data to store
	 */
	public void setData(byte[] data)
	{
		this.data = data;
	}

	/**
	 * Appends data to currently existing data
	 * @param data data to append
	 */
	public void appendData(byte[] data)
	{
		int originalLength = this.data.length;
		this.data = Arrays.copyOf(this.data, this.data.length + data.length);
		System.arraycopy(data, 0, this.data, originalLength, data.length);
	}

	/**
	 * Returns the hash stored in the object in a human readable form.
	 * Useless except for debugging
	 * @return human readable string of hash
	 */
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