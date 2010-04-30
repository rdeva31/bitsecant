package bitraptor;

import java.util.*;
import java.io.*;
import java.nio.*;

/**
 * MultiFileInfo is used to represent torrents that use multiple files.  It is essentially
 * a list of SingleFileInfo classes, with operations performed upon them.
 * @author rdeva
 */
public class MultiFileInfo extends Info
{
	private String directory = null; //Directory name to store all files in
	private List<SingleFileInfo> files = null;
	private int fileLength = 0; 	//File size

	public MultiFileInfo()
	{

	}

	public MultiFileInfo(Info i)
	{
		super(i);
	}

	public MultiFileInfo(MultiFileInfo toCopy)
	{
		directory = toCopy.directory;
		files = new ArrayList<SingleFileInfo>();
		files.addAll(toCopy.files);

		for (SingleFileInfo file : files)
		{
			fileLength += file.getFileLength();
		}
	}

	public String getDirectory()
	{
		return directory;
	}

	public void setDirectory(String directory)
	{
		this.directory = directory;
	}

	/**
	 * Returns the files with in this directory.  Note that to find the exact path
	 * of a given file f, do this:
	 * SingleFileInfo f = getFiles().get(0);
	 * System.out.println(getDirectory() + "/" + f.getName() + " is the location of file");
	 * @return all files relative to the current directory
	 */
	public List<SingleFileInfo> getFiles()
	{
		return files;
	}

	/**
	 * Sets the files associated with this torrent
	 * @param files list of files associated with this torrent
	 */
	public void setFiles(List<SingleFileInfo> files)
	{
		this.files = files;
		fileLength = 0;

		if (files != null)
		{
			for (SingleFileInfo file : files)
			{
				fileLength += file.getFileLength();
			}
		}
	}

	public int getFileLength()
	{
		return fileLength;
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
		final MultiFileInfo other = (MultiFileInfo) obj;
		if ((this.directory == null) ? (other.directory != null) : !this.directory.equals(other.directory))
		{
			return false;
		}
		if (this.files != other.files && (this.files == null || !this.files.equals(other.files)))
		{
			return false;
		}
		return true;
	}

	@Override
	public int hashCode()
	{
		int hash = 3;
		hash = 73 * hash + (this.directory != null ? this.directory.hashCode() : 0);
		return hash;
	}

	@Override
	public String toString()
	{
		String s = directory + " with:\n";
		for (SingleFileInfo f : files)
				s += "\t" + f.toString() + "\n";
		return s;
	}

	public ByteBuffer readPiece(int pieceIndex) throws Exception
	{
		//return readBlock(pieceIndex, 0, getPieceLength());
		if (pieceIndex == fileLength / getPieceLength()) //last piece
			return readBlock(pieceIndex, 0, fileLength % getPieceLength());
		else
			return readBlock(pieceIndex, 0, getPieceLength());
	}

	public ByteBuffer readBlock(int pieceIndex, int blockOffset, int blockLength) throws Exception
	{
		int limit = pieceIndex * getPieceLength() + blockOffset;
		int cumulativeFileSize = 0;
		ByteBuffer buffer = ByteBuffer.allocate(blockLength);
		Queue<SingleFileInfo> fileQueue = new LinkedList<SingleFileInfo>(files);


		for (SingleFileInfo f = fileQueue.poll(); f != null; f = fileQueue.poll())
		{
			if (cumulativeFileSize + f.getFileLength() > limit)
			{
				//now skip reading limit - cumulativeFileSize
				RandomAccessFile r = f.getFile();
				r.skipBytes(limit - cumulativeFileSize);
				byte[] b = new byte[Math.min(f.getFileLength(), blockLength)];
				int bytesRead = r.read(b);
				buffer.put(b, 0, bytesRead);

				while(bytesRead < blockLength)
				{
					f = fileQueue.poll();
					r = f.getFile();
					r.seek(0);
					if (f.getFileLength() > blockLength - bytesRead) //rest of the contents are in this file
						b = new byte[blockLength - bytesRead];
					else //entire file fits into the block but subsequent files have stuff too
						b = new byte[f.getFileLength()];

					int bytesReadTemp = r.read(b);

					if (bytesReadTemp == -1)
						throw new java.io.EOFException("didn't expect EOF");

					bytesRead += bytesReadTemp;
					buffer.put(b, 0, bytesReadTemp);
				}

				break;
			}
			else
				cumulativeFileSize += f.getFileLength();
		}



		return buffer;
	}

	public void writePiece(Piece piece) throws Exception
	{
		writeBlock(piece.getBytes(), piece.getPieceIndex(), 0, piece.getPieceLength());
	}

	public void writeBlock(byte[] data, int pieceIndex, int blockOffset, int blockLength) throws Exception
	{
		int limit = pieceIndex * getPieceLength() + blockOffset;
		int cumulativeFileSize = 0;
		Queue<SingleFileInfo> fileQueue = new LinkedList<SingleFileInfo>(files);

		for (SingleFileInfo f = fileQueue.poll(); f != null; f = fileQueue.poll())
		{
			if (cumulativeFileSize + f.getFileLength() > limit)
			{
				//now skip reading limit - cumulativeFileSize
				RandomAccessFile r = f.getFile();
				r.seek(limit - cumulativeFileSize);
				byte[] b = Arrays.copyOf(data, Math.min(f.getFileLength() - (limit - cumulativeFileSize), blockLength));
				data = Arrays.copyOfRange(data, b.length, data.length);
				int bytesWritten = b.length;

				r.write(b);

				while(bytesWritten < blockLength)
				{
					f = fileQueue.poll();
					r = f.getFile();
					r.seek(0);
					if (f.getFileLength() > blockLength - bytesWritten) //rest of the contents are in this file
						b = Arrays.copyOf(data, blockLength - bytesWritten);
					else //entire file fits into the block but subsequent files have stuff too
						b = Arrays.copyOf(data, f.getFileLength());

					data = Arrays.copyOfRange(data, b.length, data.length);
					r.write(b);
					bytesWritten += b.length;
				}

				break;
			}
			else
				cumulativeFileSize += f.getFileLength();
		}

	}

	public void finish() throws Exception
	{
		for (SingleFileInfo f : files)
		{
			f.finish();
		}
	}
}
