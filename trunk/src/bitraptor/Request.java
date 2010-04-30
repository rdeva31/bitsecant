package bitraptor;

/**
 * Represents a request made from one peer to another.
 */
public class Request
{
	private Piece piece;
	private int pieceIndex;
	private int blockOffset;
	private int blockLength;

	/**
	 * Creates a new request.
	 * @param piece the Piece buffer used when request is fulfilled
	 * @param pieceIndex index of the piece being requested
	 * @param blockOffset offset within the piece
	 * @param blockLength amount of data requested starting at that offset
	 */
	public Request(Piece piece, int pieceIndex, int blockOffset, int blockLength)
	{
		this.piece = piece;
		this.pieceIndex = pieceIndex;
		this.blockOffset = blockOffset;
		this.blockLength = blockLength;
	}

	/**
	 * Returns the Piece associated to this request
	 * @return Piece (if any)
	 */
	public Piece getPiece()
	{
		return piece;
	}

	/**
	 * Sets to the Piece associated with this request to piece.
	 * @param piece piece to be associated with this Request
	 */
	public void setPiece(Piece piece)
	{
		this.piece = piece;
	}

	/**
	 * Returns the pieceIndex used in creating this object
	 * @return index of the piece
	 */
	public int getPieceIndex()
	{
		return pieceIndex;
	}

	/**
	 * Sets the pieceIndex associated with this object to pieceIndex
	 * @param pieceIndex new pieceIndex
	 */
	public void setPieceIndex(int pieceIndex)
	{
		this.pieceIndex = pieceIndex;
	}

	/**
	 * Returns the blockOffset of this request
	 * @return blockOffset
	 */
	public int getBlockOffset()
	{
		return blockOffset;
	}

	/**
	 * Changes the blockOffset
	 * @param blockOffset new blockOffset
	 */
	public void getBlockOffset(int blockOffset)
	{
		this.blockOffset = blockOffset;
	}

	/**
	 * Returns the length of the data requested
	 * @return blockLength
	 */
	public int getBlockLength()
	{
		return blockLength;
	}

	/**
	 * Changes the size of the data requested
	 * @param blockLength new size of data
	 */
	public void setBlockLength(int blockLength)
	{
		this.blockLength = blockLength;
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
		final Request other = (Request) obj;
		if (this.pieceIndex != other.pieceIndex)
		{
			return false;
		}
		if (this.blockOffset != other.blockOffset)
		{
			return false;
		}
		if (this.blockLength != other.blockLength)
		{
			return false;
		}
		return true;
	}
}
