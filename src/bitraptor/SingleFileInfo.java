package bitraptor;

import java.util.*;
import java.io.*;
import java.nio.*;

/**
 * SingleFileInfo performs operations on torrents that use the single file mode.
 * @author rdeva
 */
public class SingleFileInfo extends Info
{
	private String name = null; 	//Filename
	private int fileLength = 0; 	//File size
	private byte [] md5sum = null; 	//MD5 hash of file
	private RandomAccessFile file;
	
	public SingleFileInfo()
	{
	}

	public SingleFileInfo(Info i)
	{
		super(i);
	}

	public SingleFileInfo(SingleFileInfo toCopy)
	{
		name = toCopy.name;
		fileLength = toCopy.fileLength;
		md5sum = Arrays.copyOf(toCopy.md5sum, toCopy.md5sum.length);
	}

	/**
	 * Returns the size of file in bytes
	 * @return size of file in bytes
	 */
	public int getFileLength()
	{
		return fileLength;
	}

	/**
	 * Sets the size of file in bytes
	 * @param fileLength size of file in bytes
	 */
	public void setFileLength(int fileLength)
	{
		this.fileLength = fileLength;
	}

	/**
	 * Returns the given md5sum of file
	 * @return 20 byte md5sum
	 */
	public byte[] getMd5sum()
	{
		return md5sum;
	}

	/**
	 * Sets the md5sum of file (succeeds iff md5sum is 20 bytes)
	 * @param md5sum md5sum of file
	 * @return true if operation succeeded; false otherwise
	 */
	public boolean setMd5sum(byte[] md5sum)
	{
		if (md5sum.length == 20)
			this.md5sum = Arrays.copyOf(md5sum, md5sum.length);
		else
			return false;
		return true;
	}

	/**
	 * Gets the name of the file.  File name may contain paths (e.g. ./folder1/folder2/file.txt)
	 * @return the name and path of file
	 */
	public String getName()
	{
		return name;
	}

	/**
	 * Sets the name of file.  File can contain paths.
	 * @param name path and name of file
	 */
	public void setName(String name)
	{
		this.name = name;
		try
		{
			finish();
			//create the directory
			if (name.lastIndexOf('/') != -1)
			{
				String directory = name.substring(0, name.lastIndexOf('/'));
				new File(directory).mkdirs();
			}
			file = new RandomAccessFile(name, "rw");
		}
		catch (Exception e)
		{
//			e.printStackTrace();
		}
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		final SingleFileInfo other = (SingleFileInfo) obj;
		if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name))
		{
			return false;
		}
		if (this.fileLength != other.fileLength)
		{
			return false;
		}
		if (!Arrays.equals(this.md5sum, other.md5sum))
		{
			return false;
		}
		return true;
	}
	
	RandomAccessFile getFile()
	{
		return file;
	}

	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 29 * hash + Arrays.hashCode(this.md5sum);
		return hash;
	}

	@Override
	public String toString()
	{
		return name + " (" + fileLength + "bytes) with md5um " + ((md5sum == null) ? "none": new String(md5sum));
	}
	
	public ByteBuffer readPiece(int pieceIndex) throws Exception
	{
		int pieceSize = getPieceLength();
		ByteBuffer buffer = ByteBuffer.allocate(pieceSize);
		byte[] data = new byte[pieceSize];
		
		file.seek(pieceIndex * pieceSize);
		file.read(data);
		
		return buffer.put(data);
	}
	
	public ByteBuffer readBlock(int pieceIndex, int blockOffset, int blockLength) throws Exception
	{
		int pieceSize = getPieceLength();
		ByteBuffer buffer = ByteBuffer.allocate(blockLength);
		byte[] data = new byte[blockLength];
		
		file.seek((pieceIndex * pieceSize) + blockOffset);
		file.read(data);
		
		return buffer.put(data);
	}
	
	public void writePiece(Piece piece) throws Exception
	{
		file.seek(piece.getPieceIndex() * getPieceLength());
		file.write(piece.getBytes());
	}
	
	public void writeBlock(byte[] data, int pieceIndex, int blockOffset, int blockLength) throws Exception
	{
		file.seek((pieceIndex * getPieceLength()) + blockOffset);
		file.write(data);
	}
	
	public void finish() throws Exception
	{
		if (file != null)
		{
			file.close();
		}
	} 
}
