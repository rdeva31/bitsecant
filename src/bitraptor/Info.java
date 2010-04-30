package bitraptor;

import java.util.*;
import java.net.*;
import java.nio.*;
import java.io.*;

/**
	Holds state about the Torrent file.
*/
public abstract class Info
{
	//Information about the torrent
	private List<URL> announceUrls = null;
	private byte[] infoHash = null;
	private long creationDate = 0;
	private String comment = null, createdBy = null, encoding = null;
	private int pieceLength; 		//Size of each piece
	private byte [] pieces; 		//SHA1 hashes for each of the pieces
	private boolean privateTorrent; 	//If true, can obtain peers only via tracker (i.e. can't use DHT etc.)

	public Info()
	{

	}

	/**
		Creates a new Info based on the parameter.
		@param toCopy - Info to copy
	*/
	public Info(Info toCopy)
	{
		this.announceUrls = toCopy.announceUrls;
		this.infoHash = (toCopy.infoHash == null) ? null : Arrays.copyOf(toCopy.infoHash, toCopy.infoHash.length);
		this.creationDate = toCopy.creationDate;
		this.comment = toCopy.comment;
		this.createdBy = toCopy.createdBy;
		this.pieceLength = toCopy.pieceLength;
		this.pieces = (toCopy.pieces == null) ? null : Arrays.copyOf(toCopy.pieces, toCopy.pieces.length);
		this.privateTorrent = toCopy.privateTorrent;
	}

	/**
		Returns the list of trackers' announce URLs associated with this torrent.
		@return List of announce URLs
	*/
	public List<URL> getAnnounceUrls()
	{
		return announceUrls;
	}

	/**
		Sets the tracker URLs associated with this torrent.  Only accepts http:// protocol.
		@param announceUrls List of torrent's announce URLs (should not be null)
	*/
	public void setAnnounceUrls(List<URL> announceUrls)
	{
		this.announceUrls = announceUrls;
	}

	/**
		Returns the comment related to this torrent
		@return the torrent's comment (can be null)
	*/
	public String getComment()
	{
		return comment;
	}

	/**
		Sets the comment related to this torrent
		@param comment - the comment (can be null)
	*/
	public void setComment(String comment)
	{
		this.comment = comment;
	}

	/**
		Returns the torrent author's name
		@return author name (can be null)
	*/
	public String getCreatedBy()
	{
		return createdBy;
	}

	/**
		Sets the author of this torrent
		@param createdBy - the author name (can be null)
	*/
	public void setCreatedBy(String createdBy)
	{
		this.createdBy = createdBy;
	}

	/**
		Returns the creation date of the torrent in unix time format
		@return torrent creation date
	*/
	public long getCreationDate()
	{
		return creationDate;
	}

	/**
		Sets the creation date of the torrent in unix time format
		@param creationDate - torrent creation date
	*/
	public void setCreationDate(long creationDate)
	{
		this.creationDate = creationDate;
	}

	/**
		Returns the encoding of the torrent file
		@return encoding (can be null)
	*/
	public String getEncoding()
	{
		return encoding;
	}

	/**
		Sets the encoding of the torrent file
		@param encoding - the encoding (can be null)
	*/
	public void setEncoding(String encoding)
	{
		this.encoding = encoding;
	}

	/**
		Returns the SHA-1 hash of info section of the torrent file
		@return SHA-1 hash
	*/
	public byte[] getInfoHash()
	{
		return infoHash;
	}

	/**
		Sets the SHA-1 hash of info section of the torrent file
		@param infoHash SHA-1 hash of the info section
	*/
	public void setInfoHash(byte[] infoHash)
	{
		this.infoHash = Arrays.copyOf(infoHash, infoHash.length);
	}

	/**
	 * Returns the size of each piece in bytes
	 * @return size of each piece
	 */
	public int getPieceLength()
	{
		return pieceLength;
	}

	/**
	 * Sets the size of each piece
	 * @param pieceLength - length of piece
	 */
	public void setPieceLength(int pieceLength)
	{
		this.pieceLength = pieceLength;
	}

	/**
	 * Returns the total number of pieces in the torrent file
	 * @return total number of pieces
	 */
	public int getTotalPieces()
	{
		return pieces.length / 20;
	}

	/**
	 * Returns the SHA-1 hash of all the pieces.  getPieces().length % 20 == true.
	 * @return the SHA hash of all the pieces
	 */
	public byte[] getPieces()
	{
		return pieces;
	}

	/**
	 * Sets the SHA-1 hash of every piece in the torrent
	 * @param pieces the hash of all the pieces concatenated
	 */
	public void setPieces(byte[] pieces)
	{
		this.pieces = Arrays.copyOf(pieces, pieces.length);
	}

	/**
	 * Defines if the torrent is allowed to contact a tracker
	 * @return true if allowed to
	 */
	public boolean isPrivateTorrent()
	{
		return privateTorrent;
	}

	/**
	 * Sets the torrent to be either private or public
	 * @param privateTorrent - true if allowed to contact tracker
	 */
	public void setPrivateTorrent(boolean privateTorrent)
	{
		this.privateTorrent = privateTorrent;
	}

	/**
	 * Returns the size of the file to be downloaded
	 * @return size of file in bytes
	 */
	public abstract int getFileLength();

	/**
	 * Essentially readBlock(pieceIndex, 0, getPieceLength())
	 * @param pieceIndex - index of the piece to read
	 * @return ByteBuffer containing the contents of the piece
	 * @throws Exception on IOException or if pieceIndex is too small or large
	 */
	public abstract ByteBuffer readPiece(int pieceIndex) throws Exception;

	/**
	 * Reads blockLength worth of data from the given pieceIndex after applying the
	 * blockOffset
	 * @param pieceIndex - index of the piece
	 * @param blockOffset - offset used after calculating the piece
	 * @param blockLength - amount of data to read
	 * @return ByteBuffer containing the read data
	 * @throws Exception on IOException or any other Exception (probably IO)
	 */
	public abstract ByteBuffer readBlock(int pieceIndex, int blockOffset, int blockLength) throws Exception;

	/**
	 * Same operation as writeBlock(data, pieceIndex, 0, getPieceLength())
	 * @param piece piece to write to the file
	 * @throws Exception on IOException or any other Exception
	 */
	public abstract void writePiece(Piece piece) throws Exception;
	
	/**
	 * Writes the given data to the given piece after applying offset.
	 * data.length == blockLength
	 * @param data - data to write
	 * @param pieceIndex - piece to write data to
	 * @param blockOffset - offset to apply after finding piece
	 * @param blockLength - data.length
	 * @throws Exception on IOException
	 */
	public abstract void writeBlock(byte[] data, int pieceIndex, int blockOffset, int blockLength) throws Exception;

	/**
	 * Cleans up internal buffers.
	 * @throws Exception on IOException e.g. if closing a file failed
	 */
	public abstract void finish() throws Exception;
	
	@Override
	public String toString()
	{
		return "announceurls:" + announceUrls.toString() +
				"; infohash:" + (infoHash == null ? "null" : infoHash) +
				"; creationdate: " + //creationDate +
				"; comment: " + comment +
				"; createdby: " + createdBy +
				"; encoding:" + encoding +
				"; piece size: " + pieceLength +
				"; private torrent: " + privateTorrent +
				"; # of pieces : " + pieces.length;
	}
	
	
}