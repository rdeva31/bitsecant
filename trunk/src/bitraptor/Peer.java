package bitraptor;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

public class Peer
{
	public enum MessageType {
		CHOKE(0),
		UNCHOKE(1),
		INTERESTED(2),
		UNINTERESTED(3),
		HAVE(4),
		BITFIELD(5),
		REQUEST(6),
		PIECE(7),
		CANCEL(8);

		private int value;
		MessageType(int value)
		{
			this.value = value;
		}

		public int valueOf()
		{
			return value;
		}

		public static MessageType fromInt(int val) throws Exception
		{
			for (MessageType m : MessageType.values())
			{
				if (val == m.value)
				{
					return m;
				}
			}

			throw new Exception("Unrecognized enum value: " + val);
		}
	};

	private static byte[] protocolName = {'B', 'i', 't', 'T', 'o', 'r', 'r', 'e', 'n', 't', ' ', 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l'};

	private Torrent torrent;
	private Info info;
	private byte[] peerID;
	private SocketChannel sock;
	private InetSocketAddress sockAddr;
	private boolean meChoking, meInterested;
	private boolean peerChoking, peerInterested;
	private BitSet pieces;
	private ByteBuffer readBuffer, writeBuffer, writeMsgBuffer;
	private int uploaded, downloaded;

	//Variables for handling requests
	private LinkedList<Request> peerRequests;
	private Request curPeerRequest;
	private ByteBuffer peerBlockBuffer;
	boolean isSendingBlock;

	private LinkedList<Request> meRequests;
	private LinkedList<Request> meSentRequests;
	private Request curMeRequest;
	private ByteBuffer mePieceBuffer;
	private ByteBuffer meBlockBuffer;
	boolean isReceivingBlock;

	public Peer(Torrent torrent, byte[] peerID, SocketChannel sock)
	{
		this(torrent, peerID, sock, (InetSocketAddress)(sock.socket().getRemoteSocketAddress()));
	}

	public Peer(Torrent torrent, byte[] peerID, String IPAddr, int port)
	{
		this(torrent, peerID, (SocketChannel)null, new InetSocketAddress(IPAddr, port));
	}

	public Peer(Torrent torrent, byte[] peerID, SocketChannel sock, InetSocketAddress sockAddr)
	{
		this.torrent = torrent;
		this.info = torrent.getInfo();
		this.peerID = peerID;
		this.sock = sock;
		this.sockAddr = sockAddr;
		meChoking = true;
		meInterested = false;
		peerChoking = true;
		peerInterested = false;
		pieces = new BitSet(info.getTotalPieces());
		readBuffer = ByteBuffer.allocate(4096);
		writeBuffer = ByteBuffer.allocate(4096);
		writeMsgBuffer = ByteBuffer.allocate(1024);
		readBuffer.order(ByteOrder.BIG_ENDIAN);
		writeBuffer.order(ByteOrder.BIG_ENDIAN);
		writeMsgBuffer.order(ByteOrder.BIG_ENDIAN);
		uploaded = 0;
		downloaded = 0;
		peerRequests = new LinkedList<Request>();
		curPeerRequest = null;
		peerBlockBuffer = null;
		isSendingBlock = false;
		meRequests = new LinkedList<Request>();
		meSentRequests = new LinkedList<Request>();
		curMeRequest = null;
		meBlockBuffer = null;
		isReceivingBlock = false;
	}

	/**
		Connects to the peer by opening a TCP connection (non blocking)
	*/
	public void connect() throws IOException
	{
		//Setting up the socket connection if necessary
		if (sock == null)
		{
			sock = SocketChannel.open();
			sock.configureBlocking(false);
			sock.socket().setReuseAddress(true);
			sock.connect(sockAddr);
		}
	}

	/**
		Gets the peer ID for the peer
		@return the peer ID
	*/
	public byte[] getPeerID()
	{
		return peerID;
	}

	/**
		Gets the socket channel for the peer connection
		@return the socket channel
	*/
	public SocketChannel getSocket()
	{
		return sock;
	}

	/**
		Gets the socket address for the remote peer
		@return the remote socket address
	*/
	public InetSocketAddress getSockAddr()
	{
		return sockAddr;
	}

	/**
		Sets if we are choking or unchoking the peer (and sends the peer a message)
		@param choking if we are choking the peer
	*/
	public void setChoking(boolean choking)
	{
		if (choking && !meChoking)
		{
			writeMessage(MessageType.CHOKE, null);
		}
		else if (!choking && meChoking)
		{
			writeMessage(MessageType.UNCHOKE, null);
		}

		meChoking = choking;
	}

	/**
		Sets if we are interested or uninterested in the peer (and sends the peer a message)
		@param interested if we are interested in the peer
	*/
	public void setInterested(boolean interested)
	{
		if (interested && !meInterested)
		{
			writeMessage(MessageType.INTERESTED, null);
		}
		else if (!interested && meInterested)
		{
			writeMessage(MessageType.UNINTERESTED, null);
		}

		meInterested = interested;
	}

	/**
		Gets if we are choking the peer
		@return if we are choking the peer
	*/
	public boolean isChoking()
	{
		return meChoking;
	}

	/**
		Gets if we are interested in the peer
		@return if we are interested in the peer
	*/
	public boolean isInterested()
	{
		return meInterested;
	}

	/**
		Sets whether the peer is choking us or not
		@param interested if the peer is interested in us
	*/
	public void setPeerChoking(boolean choking)
	{
		peerChoking = choking;
	}

	/**
		Sets whether the peer is interested in us or not
		@param interested if the peer is interested in us
	*/
	public void setPeerInterested(boolean interested)
	{
		peerInterested = interested;
	}

	/**
		Gets if the peer is choking us
		@return if we are being choked
	*/
	public boolean isPeerChoking()
	{
		return peerChoking;
	}

	/**
		Gets if the peer is interested in us
		@return if the peer is interested in us
	*/
	public boolean isPeerInterested()
	{
		return peerInterested;
	}

	/**
		Gets the BitSet for the pieces the peer has
		@return BitSet of peer pieces
	*/
	public BitSet getPieces()
	{
		return pieces;
	}

	/**
		Gets the read buffer for reading in data
		@return read buffer
	*/
	public ByteBuffer getReadBuffer()
	{
		return readBuffer;
	}

	/**
		Gets the write buffer for writing out data
		@return write buffer
	*/
	public ByteBuffer getWriteBuffer()
	{
		return writeBuffer;
	}

	/**
		Gets the write message buffer for sending messages
		@return write message buffer
	*/
	public ByteBuffer getWriteMsgBuffer()
	{
		return writeMsgBuffer;
	}

	/**
		Checks if the peer is currently handling any requests (sent or not)
		@return if handling any requests
	*/
	public boolean isHandlingRequest()
	{
		return ((meRequests.size() > 0) || (meSentRequests.size() > 0));
	}

	/**
		Gets the total amount of data uploaded to the peer since last reset
		@return uploaded total
	*/
	public int getUploaded()
	{
		return uploaded;
	}

	/**
		Resets the upload total
	*/
	public void resetUploaded()
	{
		uploaded = 0;
	}

	/**
		Gets the total amount of data downloaded the peer since last reset
		@return downloaded total
	*/
	public int getDownloaded()
	{
		return downloaded;
	}

	/**
		Resets the download total
	*/
	public void resetDownloaded()
	{
		downloaded = 0;
	}

	/**
		Gets the number of requests currently queued
		@return number of queued requests
	*/
	public int getNumRequests()
	{
		return meRequests.size();
	}

	/**
		Adding a request to the queue if not already in queue or sent
		@param request request to add
	*/
	public void addRequest(Request request)
	{
		if((!meRequests.contains(request)) && (!meSentRequests.contains(request)))
		{
			meRequests.add(request);
		}
	}

	/**
		Adding a collection of requests to the queue if not already in queue or sent
		@param requests requests to add
	*/
	public void addRequests(Collection<Request> requests)
	{
		for (Request request : requests)
		{
			if((!meRequests.contains(request)) && (!meSentRequests.contains(request)))
			{
				meRequests.add(request);
			}
		}
	}

	/**
		Getting the queued requests
		@return queued requests
	*/
	public LinkedList<Request> getRequests()
	{
		return meRequests;
	}

	/**
		Getting the requests that were sent to the peer
		@return sent requests
	*/
	public LinkedList<Request> getSentRequests()
	{
		return meSentRequests;
	}

	/**
		Removing a request from either the queued
		@param request request to remove
	*/
	public void removeRequest(Request request)
	{
		meRequests.remove(request);
	}

	/**
		Randomizing the order of the requests in the queue
	*/
	public void shuffleRequests()
	{
		Collections.shuffle(meRequests);
	}

	/**
		Writing a message to the message buffer
		@param type type of message
		@param payload payload for the message
	*/
	public void writeMessage(MessageType type, ByteBuffer payload)
	{
		if (payload != null)
		{
			payload.flip();
			writeMsgBuffer.putInt(1 + payload.remaining()).put((byte)type.valueOf()).put(payload);
		}
		else
		{
			writeMsgBuffer.putInt(1).put((byte)type.valueOf());
		}
	}

	/**
		Writing a message to the message buffer that has a specified length of bytes, assuming that the
		bytes that are not currently in payload will be sent later. Used for PIECE messages to peers.
		@param type type of message
		@param payload payload for the message
		@param additionalLength number of bytes that will come later in the message, that are not present in the payload
	*/
	public void writeMessage(MessageType type, ByteBuffer payload, int additionalLength)
	{
		if (payload != null)
		{
			payload.flip();
			writeMsgBuffer.putInt(1 + payload.remaining() + additionalLength).put((byte)type.valueOf()).put(payload);
		}
		else
		{
			writeMsgBuffer.putInt(1 + additionalLength).put((byte)type.valueOf());
		}
	}

	/**
		Checking the handshake message that was sent from the peer to ensure validity
	*/
	public boolean checkHandshake() throws Exception
	{
		//Flipping the buffer to read the data
		readBuffer.flip();

		//Dropping the connection if invalid name length
		if (readBuffer.get() != 19)
		{
			readBuffer.clear();
			return false;
		}

		//Dropping the connection if invalid protocol name
		byte[] name = new byte[19];
		readBuffer.get(name);
		for (int b = 0; b < 19; b++)
		{
			if (protocolName[b] != name[b])
			{
				readBuffer.clear();
				return false;
			}
		}

		//Skipping over the next 8 reserved bytes
		readBuffer.getDouble();

		//Getting the info hash and peer ID
		byte[] infoHash = new byte[20];
		readBuffer.get(infoHash);
		readBuffer.get(peerID);

		//Dropping the connection if the info hash does not match
		if (!Arrays.equals(info.getInfoHash(), infoHash))
		{
			readBuffer.compact();
			return false;
		}

		//Dropping the connection if the peer ID matches a current peer's peer ID
		LinkedList<Peer> peerSet = torrent.getPeers();
		for (Peer peer : peerSet)
		{
			if((peer != this) && (Arrays.equals(peer.getPeerID(), peerID)))
			{
				throw new Exception("Already have a peer with that peer ID");
			}
		}

		readBuffer.compact();
		return true;
	}

	/**
		Sets up the write buffer based on messages or piece data. Also sends requests if needed to the peer
	*/
	public void setupWrites() throws Exception
	{
		//Currently sending a block to the peer, so copy data from the bock buffer
		if (isSendingBlock)
		{
			int startingPosition = peerBlockBuffer.position();

			try
			{
				writeBuffer.put(peerBlockBuffer);
			}
			catch (BufferOverflowException e)
			{
				int prevLimit = peerBlockBuffer.limit();
				peerBlockBuffer.limit(startingPosition + writeBuffer.remaining());
				writeBuffer.put(peerBlockBuffer);
				peerBlockBuffer.limit(prevLimit);
			}

			//Adding to uploaded total
			uploaded += (peerBlockBuffer.position() - startingPosition);

			//No more block data to send, so end the current request
			if (!peerBlockBuffer.hasRemaining())
			{
//				System.out.println("[SERVED] Peer " + sockAddr);
				curPeerRequest = null;
				isSendingBlock = false;
			}
		}
		//Not sending a block
		else
		{
			//Sending up to 2 requests to the peer if possible and none were sent yet still unfulfilled
			if((!peerChoking) && (meSentRequests.size() == 0) && (meRequests.size() > 0))
			{
				int requestsToSend = Math.min(2, meRequests.size());
				for (int r = 0; r < requestsToSend; r++)
				{
					Request newRequest = meRequests.remove();

					ByteBuffer header = ByteBuffer.allocate(12);
					header.order(ByteOrder.BIG_ENDIAN);
					header.putInt(newRequest.getPieceIndex());
					header.putInt(newRequest.getBlockOffset());
					header.putInt(newRequest.getBlockLength());

					writeMessage(MessageType.REQUEST, header);

					meSentRequests.add(newRequest);

//					System.out.println("[SEND REQUEST] Piece #" + newRequest.getPieceIndex() + " block offset " + newRequest.getBlockOffset());
				}
			}

			//There is a queued request from the peer
			if ((peerRequests.size() > 0) && !meChoking)
			{
				curPeerRequest = peerRequests.remove();

				int blockLength = curPeerRequest.getBlockLength();
				int pieceIndex = curPeerRequest.getPieceIndex();
				int blockOffset = curPeerRequest.getBlockOffset();

				//Reading in the block that the peer wants from the file
				peerBlockBuffer = info.readBlock(pieceIndex, blockOffset, blockLength);
				peerBlockBuffer.flip();

				//Setting up the header
				ByteBuffer header = ByteBuffer.allocate(8);
				header.order(ByteOrder.BIG_ENDIAN);
				header.putInt(pieceIndex);
				header.putInt(blockOffset);

				writeMessage(MessageType.PIECE, header, blockLength);

//				System.out.println("[HANDLING PEER REQUEST] Peer " + sockAddr);

				isSendingBlock = true;
			}

			//Copy messages from the message buffer to the write buffer
			writeMsgBuffer.flip();

			try
			{
				writeBuffer.put(writeMsgBuffer);
			}
			catch (BufferOverflowException e)
			{
				int prevLimit = writeMsgBuffer.limit();
				writeMsgBuffer.limit(writeBuffer.remaining());
				writeBuffer.put(writeMsgBuffer);
				writeMsgBuffer.limit(prevLimit);
			}

			writeMsgBuffer.compact();
		}
	}

	/**
		Handles messages that were received from the peer
	*/
	public void handleMessages() throws Exception
	{
		//Flipping the buffer to read from it
		readBuffer.flip();

		//Currently receiving a block from the peer
		if (isReceivingBlock)
		{
			try
			{
				meBlockBuffer.put(readBuffer);
			}
			catch (BufferOverflowException e)
			{
				int prevLimit = readBuffer.limit();
				readBuffer.limit(meBlockBuffer.remaining());
				meBlockBuffer.put(readBuffer);
				readBuffer.limit(prevLimit);
			}

			//Adding to downloaded total
			downloaded += readBuffer.position();

			//Block was fully downloaded from the peer
			if (!meBlockBuffer.hasRemaining())
			{
				//Removing from sent requests
				meSentRequests.remove(curMeRequest);

				//Letting the torrent know it finished the request (for end game mode)
				torrent.finishRequest(curMeRequest);

				//Writing the block to the piece
				meBlockBuffer.flip();
				byte[] block = new byte[meBlockBuffer.remaining()];
				meBlockBuffer.get(block);
				boolean isPieceFinished = curMeRequest.getPiece().writeBlock(curMeRequest.getBlockOffset(), block);

				//Piece is finished downloading, so notify the torrent to do final processing
				if (isPieceFinished)
				{
					torrent.finishPiece(curMeRequest.getPiece());
				}

				//Sending a new request if possible
				if (!peerChoking && meRequests.size() > 0)
				{
					Request newRequest = meRequests.remove();

					ByteBuffer header = ByteBuffer.allocate(12);
					header.order(ByteOrder.BIG_ENDIAN);
					header.putInt(newRequest.getPieceIndex());
					header.putInt(newRequest.getBlockOffset());
					header.putInt(newRequest.getBlockLength());

					writeMessage(MessageType.REQUEST, header);

					meSentRequests.add(newRequest);

//					System.out.println("[SEND REQUEST] Piece #" + newRequest.getPieceIndex() + " block offset " + newRequest.getBlockOffset());
				}

				isReceivingBlock = false;
			}
		}
		//Not receiving a block from the peer
		else
		{
			//Going through all the messages from the peer
			while (readBuffer.remaining() >= 4)
			{
				int messageLength = readBuffer.getInt();

				//Keep alive message
				if (messageLength == 0)
				{
					continue;
				}

				//Not enough available to read the message type, so wait until next time
				if (readBuffer.remaining() == 0)
				{
					readBuffer.position(readBuffer.position() - 4);
					break;
				}

				MessageType messageID = MessageType.fromInt(readBuffer.get());

				//Making sure that the buffer has the full message (or atleast enough for piece message information)
				if ((messageLength - 1) <= readBuffer.remaining() ||
					((messageID == MessageType.PIECE) && (readBuffer.remaining() >= 8)))
				{
//					System.out.println("\tMessage ID: " + messageID);

					//Handling the message based on the ID
					switch (messageID)
					{
						case CHOKE:
							//Sending cancel messages to the peer for each sent request
							ByteBuffer payload = ByteBuffer.allocate(12);
							payload.order(ByteOrder.BIG_ENDIAN);
							for (Request request : meSentRequests)
							{
								payload.clear();
								payload.putInt(request.getPieceIndex());
								payload.putInt(request.getBlockOffset());
								payload.putInt(request.getBlockLength());

								writeMessage(MessageType.CANCEL, payload);
							}

							//Adding all the requests to the torrent request pool
							torrent.addRequestsToPool(meRequests);
							meRequests.clear();
							torrent.addRequestsToPool(meSentRequests);
							meSentRequests.clear();

//							System.out.println("[CHOKED] By peer " + sockAddr);
							peerChoking = true;
							break;

						case UNCHOKE:
//							System.out.println("[UNCHOKED] By peer " + sockAddr);
							peerChoking = false;
							break;

						case INTERESTED:
							peerInterested = true;
							break;

						case UNINTERESTED:
							peerInterested = false;
							break;

						case HAVE:
						{
							int piece = readBuffer.getInt();
							pieces.set(piece);

							//Advertising interest if it is a piece we do not have / have not requested
							if(!torrent.getRequestedPieces().get(piece))
							{
								setInterested(true);
							}

							break;
						}

						case BITFIELD:
						{
							byte[] bitField = new byte[messageLength - 1];
							readBuffer.get(bitField);
							pieces.clear();

							//Initializing the bitfield based off of the bytes
							int totalPieces = info.getTotalPieces();
							int pieceIndex = 0;
							for (byte b : bitField)
							{
								for (int c = 7; (c >= 0) && (pieceIndex < totalPieces); c--)
								{
									if ((b & (1 << c)) != 0)
									{
										pieces.set(pieceIndex);
									}
									pieceIndex++;
								}
							}

							//Advertising interest if there is a piece we do not have / have not requested
							pieceIndex = -1;
							while ((pieceIndex = pieces.nextSetBit(pieceIndex + 1)) != -1)
							{
								if(!torrent.getRequestedPieces().get(pieceIndex))
								{
									setInterested(true);
									break;
								}
							}

							break;
						}

						case REQUEST:
						{
							int pieceIndex = readBuffer.getInt();
							int blockOffset = readBuffer.getInt();
							int blockLength = readBuffer.getInt();

							byte block[] = new byte[blockLength];

							//Do not queue the request if we are choking them
							if (meChoking)
							{
								break;
							}
							//Do not queue the request if it is for a piece we do not have yet
							else if (!torrent.getReceivedPieces().get(pieceIndex))
							{
								break;
							}
							//Block length is > 128 KB, so drop the connection
							else if (blockLength > (128 * 1024))
							{
								throw new Exception("Requested blocksize too big");
							}

//							System.out.println("[PEER REQUEST]");

							peerRequests.add(new Request(null, pieceIndex, blockOffset, blockLength));

							break;
						}

						case PIECE:
						{
							int pieceIndex = readBuffer.getInt();
							int blockOffset = readBuffer.getInt();

							//Attempting to find the request that corresponds to the piece message
							curMeRequest = null;
							for(Request request : meSentRequests)
							{
								if ((pieceIndex == request.getPieceIndex()) && (blockOffset == request.getBlockOffset()))
								{
									curMeRequest = request;
								}
							}

							//This piece was not matched up to a request
							if (curMeRequest == null)
							{
								throw new Exception("Invalid piece message");
							}

//							System.out.println("[RECEIVE PIECE] Piece # " + pieceIndex + " block offset " + curMeRequest.getBlockOffset() + " from peer " + sockAddr);

							//Setting up to receive the block
							meBlockBuffer = ByteBuffer.allocate(curMeRequest.getBlockLength());
							isReceivingBlock = true;

							//Enabling the buffer to be written to again (since returning immediately)
							readBuffer.compact();

							return;
						}

						case CANCEL:
						{
							int pieceIndex = readBuffer.getInt();
							int blockOffset = readBuffer.getInt();
							int blockLength = readBuffer.getInt();

							peerRequests.remove(new Request(null, pieceIndex, blockOffset, blockLength));
							break;
						}
					}
				}
				else
				{
					readBuffer.position(readBuffer.position() - 5);
					break;
				}
			}
		}

		//Enabling the buffer to be written to again
		readBuffer.compact();
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
		final Peer other = (Peer) obj;
		if (!peerID.equals(other.peerID))
		{
			return false;
		}
		return true;
	}

	@Override
	public int hashCode()
	{
		int hash = 3;
		hash = 53 * hash + Arrays.hashCode(peerID);
		return hash;
	}
}