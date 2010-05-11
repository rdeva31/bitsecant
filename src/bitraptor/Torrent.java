package bitraptor;

import java.util.*;
import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.security.MessageDigest;
import org.klomp.snark.bencode.*;
import chord.*;

public class Torrent
{
	private enum State { STARTED, RUNNING, STOPPED, COMPLETED };
	
	private static byte[] protocolName = {'B', 'i', 't', 'T', 'o', 'r', 'r', 'e', 'n', 't', ' ', 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l'};

	private Info info = null;
	private int port;
	private byte[] peerID;
	private String trackerID = null;
	private Selector handshakeSelect;
	private Selector select;
	private Chord chord;
	private List<RaptorData.PeerData> peerList;
	private HashMap<SocketChannel, Peer> peers;
	private HashMap<Integer, Piece> pieces;
	private HashMap<Peer, Boolean> uploadSlotActions;
	private BitSet receivedPieces;
	private BitSet requestedPieces;
	private LinkedList<Request> requestPool;
	private State state = State.STARTED;
	private boolean inEndGameMode;

	private Timer DHTAnnouncerTimer;

	private final int SEEDING_SLOT_ASSIGN_TIMER_PERIOD = 10*1000; //in milliseconds
	private final int SLOT_ASSIGN_TIMER_PERIOD = 10*1000; //in milliseconds
	private final int NUM_UPLOAD_SLOTS = 5; //number of upload slots
	private final int BLOCK_SIZE = 16*1024;

	private final int REQUEST_TIMER_PERIOD = 100; //in milliseconds
	private boolean requestTimerStatus = false;

	private final int END_GAME_REQUEST_TIMER_PERIOD = 30*1000; //in milliseconds
	private boolean endGameRequestTimerStatus = false;

	private final int DHT_ANNOUNCE_TIMER_PERIOD = 60; //in seconds
	private final int PEER_EXPIRER_TIMER_PERIOD = 5*1000; //in milliseconds
	private final int INITIAL_TTL = (int)((DHT_ANNOUNCE_TIMER_PERIOD * 1.5) / (PEER_EXPIRER_TIMER_PERIOD / 1000)); //in seconds

	/**
		Initializes the Torrent based on the information from the file.
		@param info Contains torrent characteristics
		@param port Port number between 1024-65535
	*/
	public Torrent(Info info, int port) throws Exception
	{
		this.port = port;
		this.info = info;

		//Creating a random peer ID (BRXXX...)
		peerID = new byte[20];
		(new Random()).nextBytes(peerID);

		peerID[0] = (byte)'B';
		peerID[1] = (byte)'R';
		peerID[2] = (byte)'-';

		//Change the random bytes into numeric characters
		for (int b = 3; b < 20; b++)
		{
			peerID[b] = (byte)(((peerID[b] & 0xFF) % 10) + 48);
		}

		peers = new HashMap<SocketChannel, Peer>();
		pieces = new HashMap<Integer, Piece>();
		uploadSlotActions = new HashMap<Peer, Boolean>();
		requestedPieces = new BitSet(info.getTotalPieces());
		receivedPieces = new BitSet(info.getTotalPieces());
		requestPool = new LinkedList<Request>();

		inEndGameMode = false;

		//Setting up selectors
		try
		{
			handshakeSelect = Selector.open();
			select = Selector.open();
		}
		catch (Exception e)
		{
			System.out.println("ERROR: Could not open selectors for use in torrent");
			System.exit(-1);
		}

		//Join the ring
		chord = new Chord(port);
		for (InetAddress IPAddr : info.getNodes().keySet())
		{
			try
			{
				ChordNode n = new ChordNode(IPAddr, info.getNodes().get(IPAddr).shortValue());
				chord.join(n);
				chord.listen();
				break;
			}
			catch (Exception e)
			{
				continue;
			}
		}
	}

	/**
		Gets the information on the torrent
		@return torrent information
	*/
	public Info getInfo()
	{
		return info;
	}

	/**
		Gets the peers that the torrent currently holds
		@return peers
	*/
	public LinkedList<Peer> getPeers()
	{
		synchronized(peers)
		{
			return new LinkedList<Peer>(peers.values());
		}
	}

	/**
		Gets the number of peers the torrent currently holds
		@return number of peers
	*/
	public int getTotalPeers()
	{
		synchronized(peers)
		{
			return peers.values().size();
		}
	}

	/**
		Gets the BitSet representing the pieces that were requested
		@return requested pieces BitSet
	*/
	public BitSet getRequestedPieces()
	{
		return requestedPieces;
	}

	/**
		Gets the BitSet representing the pieces that were fully received
		@return received pieces BitSet
	*/
	public BitSet getReceivedPieces()
	{
		return receivedPieces;
	}

	/**
		Checks if the torrent is currently in end game mode
		@return end game mode status
	*/
	public boolean isInEndGameMode()
	{
		return inEndGameMode;
	}

	/**
		Finds the next optimal piece to request relative to a given peer
		@param peer The peer to search relative to
		@returns The piece index or -1 if no piece found
	*/
	private int getNextPiece(Peer peer)
	{
		BitSet pieces = ((BitSet)peer.getPieces().clone());
		pieces.andNot(requestedPieces);
		int[] pieceCounts = new int[pieces.length()];

		//No piece found if all the pieces the peer has are requested (or if peer had no pieces)
		if(pieces.isEmpty())
		{
			return -1;
		}

		//Initialize piece counts based on what the peer has
		for (int c = 0; c < pieceCounts.length; c++)
		{
			if (pieces.get(c))
			{
				pieceCounts[c] = 1;
			}
			else
			{
				pieceCounts[c] = 0;
			}
		}

		//Adding to piece counts based on shared piecess
		LinkedList<Peer> peerSet = getPeers();
		for (Peer p : peerSet)
		{
			//Skipping over the peer itself since it was already taken into account
			if (p.equals(peer))
			{
				continue;
			}

			//Finding the pieces shared between the peers
			BitSet sharedPieces = (BitSet)p.getPieces().clone();
			sharedPieces.and(pieces);

			//Incrementing the shared pieces in the counts array
			int curPiece = -1;
			while ((curPiece = sharedPieces.nextSetBit(curPiece + 1)) != -1)
			{
				pieceCounts[curPiece] += 1;
			}
		}

		//Finding the smallest count (greater than 0) and all of the pieces that have that count value
		int lowestCount = Integer.MAX_VALUE;
		LinkedList<Integer> bestPieces = new LinkedList<Integer>();
		int curPieceIndex = -1;

		while ((curPieceIndex = pieces.nextSetBit(curPieceIndex + 1)) != -1)
		{
			if ((pieceCounts[curPieceIndex] > 0) && (pieceCounts[curPieceIndex] < lowestCount))
			{
				lowestCount = pieceCounts[curPieceIndex];
				bestPieces.clear();
				bestPieces.add(curPieceIndex);
			}
			else if (pieceCounts[curPieceIndex] == lowestCount)
			{
				bestPieces.add(curPieceIndex);
			}
		}

		//Choosing a random piece out of the ones that share the lowest count
		int pieceIndex = bestPieces.get((int)(Math.random() * (bestPieces.size() - 1)));

		return pieceIndex;
	}

	/**
		Finishes a block request by removing the request from any other peers if in end game mode
		@param request request to remove
	*/
	public void finishRequest(Request request)
	{
		//Only interested if the torrent is in end game mode
		if (!inEndGameMode)
		{
			return;
		}

		//Going through all the peers and removing the request
		LinkedList<Peer> peerSet = getPeers();
		for (Peer peer : peerSet)
		{
			peer.removeRequest(request);
		}
	}

	/**
		Finishes a piece download by checking the hash, writing to the file, and sending out HAVEs
		@param request request to remove
	*/
	public void finishPiece(Piece piece)
	{
		int pieceIndex = piece.getPieceIndex();
		byte[] downloadedHash = null;

		try
		{
			downloadedHash = piece.hash();
		}
		catch (Exception e)
		{
			pieces.remove(pieceIndex);
			requestedPieces.clear(pieceIndex);
			return;
		}

		//The hash of the downloaded piece matches the known hash
		if(Arrays.equals(downloadedHash, Arrays.copyOfRange(info.getPieces(), pieceIndex * 20, (pieceIndex + 1) * 20)))
		{
			try
			{
				info.writePiece(piece);
			}
			catch (Exception e)
			{
				System.out.println("ERROR: Could not write piece to file");
				System.exit(-1);
			}

			receivedPieces.set(pieceIndex);
			pieces.remove(pieceIndex);

//			System.out.println("[DOWNLOADED] " + pieceIndex);

			//Sending out HAVE messages to all peers for the new piece
			ByteBuffer payload = ByteBuffer.allocate(4);
			payload.order(ByteOrder.BIG_ENDIAN);

			LinkedList<Peer> peerSet = getPeers();
			for (Peer peer : peerSet)
			{
				payload.clear();
				payload.putInt(pieceIndex);

				try
				{
					peer.writeMessage(Peer.MessageType.HAVE, payload);
				}
				//Peer's write buffer was full, so removing the peer
				catch (Exception e)
				{
					forceRemovePeer(peer);
				}
			}
		}
		//The hashes do not match
		else
		{
//			System.out.println("[HASH FAIL] " + pieceIndex);
			pieces.remove(pieceIndex);
			requestedPieces.clear(pieceIndex);
		}
	}

	/**
		Adding an array of requests to the request pool
		@param requests to add to the pool
	*/
	public void addRequestsToPool(LinkedList<Request> requests)
	{
		requestPool.addAll(requests);
	}

	/**
		Generating requests and adding them to optimally selected peers to distribute the load
		@param p piece index of the piece to add requests for
		@param peer peer to add the requests to
	*/
	public void generateRequests(int p, Peer peer)
	{
		//Calculating the piece length (possibly shorter if it is the last piece)
		int pieceLength = info.getPieceLength();
		if (p == (info.getTotalPieces() - 1))
		{
			pieceLength = info.getFileLength() - (p * pieceLength);
		}

		//Initializing a piece and adding it to the map of pieces
		Piece piece = new Piece(p, pieceLength);
		pieces.put(p, piece);

		//Getting the list of requests for the piece and adding them to the peer
		LinkedList<Request> requests = piece.getUnfulfilledRequests(BLOCK_SIZE);
		peer.addRequests(requests);

//		System.out.println("[ADD REQUEST] Piece #" + p + " from " + peer.getSockAddr());
	}

	/**
		Starts downloading the torrent and handles the main interactions
	*/
	public void start()
	{
		//Allowing seeding/downloading to continue if the files exist
		for(int p = 0; p < info.getTotalPieces(); p++)
		{
			//Reading the piece in from the file and calculating the hash
			byte[] hash;

			try
			{
				ByteBuffer piece = info.readPiece(p);
				piece.flip();
				byte[] pieceData = new byte[piece.remaining()];
				piece.get(pieceData);

				hash = MessageDigest.getInstance("SHA-1").digest(pieceData);
			}
			catch (Exception e)
			{
				continue;
			}

			//The hash of the downloaded piece matches the known hash
			if(Arrays.equals(hash, Arrays.copyOfRange(info.getPieces(), p * 20, (p + 1) * 20)))
			{
				receivedPieces.set(p);
				requestedPieces.set(p);
			}
		}

		//Starting up the announcer (runs immediately and then it internally handles scheduling)
		(new DHTAnnouncer(this)).run();

		//Starting up the upload slot assign timer
		Timer uploadSlotTimer = new Timer(false);
		uploadSlotTimer.scheduleAtFixedRate(new UploadSlotAssigner(NUM_UPLOAD_SLOTS), 0, SLOT_ASSIGN_TIMER_PERIOD);

		//Starting up the request timer
		Timer requestTimer = new Timer(false);
		requestTimer.scheduleAtFixedRate(new RequestTimer(), 0, REQUEST_TIMER_PERIOD);

		//Initializing the end game request timer
		Timer endGameRequestTimer = null;

		//Starting up the peer expirer timer
		Timer peerExpirerTimer = new Timer(false);
		peerExpirerTimer.scheduleAtFixedRate(new ExpirePeersTimer(), PEER_EXPIRER_TIMER_PERIOD, PEER_EXPIRER_TIMER_PERIOD);

		//Looping forever
		while(true)
		{
			//Handling any upload slot actions that were generated
			if (uploadSlotActions.size() > 0)
			{
				synchronized(uploadSlotActions)
				{
					Set<Peer> peerSet = uploadSlotActions.keySet();
					for (Peer peer : peerSet)
					{
						peer.setChoking(uploadSlotActions.get(peer));
					}

					uploadSlotActions.clear();
				}
			}

			//Downloaded all of the pieces
			if ((receivedPieces.cardinality() == info.getTotalPieces()) && (state != State.COMPLETED))
			{
				System.out.println("Finished Downloading!");

				state = State.COMPLETED;

				//Removing all seeder peers, since they are not needed any more
				synchronized (peers)
				{
					LinkedList<Peer> peerSet = new LinkedList<Peer>(peers.values());
					for (Peer peer : peerSet)
					{
						if (peer.getPieces().cardinality() == info.getTotalPieces())
						{
							forceRemovePeer(peer);
						}
					}
				}

				//Canceling the request timer and end game request timer
				//requestTimer.cancel();
				if (endGameRequestTimer != null)
				{
					endGameRequestTimer.cancel();
				}

				//Setting the upload slot timer to instead run every 30 seconds
				uploadSlotTimer.cancel();
				uploadSlotTimer = new Timer(false);
				uploadSlotTimer.scheduleAtFixedRate(new UploadSlotAssigner(NUM_UPLOAD_SLOTS), 0, SEEDING_SLOT_ASSIGN_TIMER_PERIOD);
			}

			//Checking to see if the torrent can start end game mode
			if (((requestedPieces.cardinality() >= ((double)info.getTotalPieces() * 0.95)) && (!inEndGameMode) && (state != State.COMPLETED))
				|| (inEndGameMode && endGameRequestTimerStatus))
			{
				//Previously not in end game mode
				if (!inEndGameMode)
				{
//					System.out.println("***ENTERING END GAME MODE***");

					inEndGameMode = true;

					//Starting up the end game mode request timer
					endGameRequestTimer = new Timer();
					endGameRequestTimer.scheduleAtFixedRate(new EndGameRequestTimer(), END_GAME_REQUEST_TIMER_PERIOD, END_GAME_REQUEST_TIMER_PERIOD);
				}
				else
				{
					endGameRequestTimerStatus = false;
//					System.out.println("***END GAME PROCESSING***");
				}

				//Getting the set of peers
				LinkedList<Peer> peerSet = getPeers();

				//Dropping all of the queued but not sent requests for each peer
				for (Peer peer : peerSet)
				{
					peer.getRequests().clear();
				}

				//Calculating the unfulfilled requests
				BitSet unfulfilledPieces = ((BitSet)receivedPieces.clone());
				unfulfilledPieces.flip(0, info.getTotalPieces());

				//Going through all of the unfulfilled pieces and adding them to a list
				int pieceIndex = -1;
				int totalPieces = info.getTotalPieces();
				LinkedList<Piece> pieceList = new LinkedList<Piece>();
				while (((pieceIndex = unfulfilledPieces.nextSetBit(pieceIndex + 1)) != -1))
				{
//					System.out.println("UNFULFILLED PIECE: " + pieceIndex);

					//Went past the last actual piece, so stop searching
					if (pieceIndex >= totalPieces)
					{
						break;
					}

					//Finding the corresponding piece to the index
					Piece piece = pieces.get(pieceIndex);

					//If the piece does not exist yet, initialize and store it
					if (piece == null)
					{
						int pieceLength = info.getPieceLength();
						if (pieceIndex == (totalPieces - 1))
						{
							pieceLength = info.getFileLength() - (pieceIndex * pieceLength);
						}

						piece = new Piece(pieceIndex, pieceLength);
						pieces.put(pieceIndex, piece);

						requestedPieces.set(pieceIndex);
					}

					//Getting the list of requests for the piece
					LinkedList<Request> requests = piece.getUnfulfilledRequests(BLOCK_SIZE);

					//Sending out the requests to all of the peers that have the piece (make sure they know you are interested too)
					for (Peer peer : peerSet)
					{
						if(peer.getPieces().get(pieceIndex))
						{
//							System.out.println("\tPeer Request: " + peer.getSockAddr());
							peer.setInterested(true);
							peer.addRequests(requests);
						}
					}
				}

				//Forcing each peer to shuffle the order of their requests in order to increase speed
				for (Peer peer : peerSet)
				{
					peer.shuffleRequests();
				}
			}

			//Add requests from the request pool to random peers
			LinkedList<Request> tempPool = new LinkedList<Request>(requestPool);
			for (Request request : tempPool)
			{
				int pieceIndex = request.getPieceIndex();

				//Going through all the peers
				LinkedList<Peer> peerSet = getPeers();
				LinkedList<Peer> peersWithPiece;
				peersWithPiece = new LinkedList<Peer>();
				for (Peer peer : peerSet)
				{
					//Peer has the piece
					if (peer.getPieces().get(pieceIndex))
					{
						peersWithPiece.add(peer);
					}
				}

				//Letting the request sit in the pool since no peer can handle it
				if (peersWithPiece.size() == 0)
				{
					continue;
				}

				//In end game mode, so send it to all possible peers
				if (inEndGameMode)
				{
					for (Peer peer : peersWithPiece)
					{
						peer.setInterested(true);
						peer.addRequest(request);
					}
				}
				//Not in end game mode, so choose a random peer
				else
				{
					//Choosing the random peer to add the request to
					Peer randomPeer = peersWithPiece.get((int)(Math.random() * (peersWithPiece.size() - 1)));
					randomPeer.setInterested(true);
					randomPeer.addRequest(request);
				}

				//Removing the request from the pool
				requestPool.remove(request);
			}

			//Generate new piece requests as needed
			if (requestTimerStatus)
			{
				requestTimerStatus = false;

				LinkedList<Peer> peerSet = getPeers();
				for (Peer peer : peerSet)
				{
					//Skipping over any peers with requests already or peers that are choking us
					if((peer.getNumRequests() > 2) || peer.isPeerChoking())
					{
						continue;
					}

					//Finding the optimal piece to request
					int p = getNextPiece(peer);

					//No piece found that we want to request from peer
					if (p == -1)
					{
						continue;
					}

//					System.out.println("[Piece Request] To peer " + peer.getSockAddr() + " for piece # " + p);

					//Generating all of the requests and adding them to the peer
					generateRequests(p, peer);

					//Set that the piece was requested
					requestedPieces.set(p);
				}
			}

			//Handshake Selector
			try
			{
				//Performing the select
				//handshakeSelect.selectNow();
				handshakeSelect.select(100);
				Iterator it = handshakeSelect.selectedKeys().iterator();

				while(it.hasNext())
				{
					SelectionKey selected = (SelectionKey)it.next();
					it.remove();

					//Getting the peer associated with the socket channel
					SocketChannel sock = (SocketChannel)selected.channel();
					Peer peer;

					synchronized(peers)
					{
						 peer = peers.get(sock);
					}

					//Peer does not exist, so remove the entry from the selector
					if (peer == null)
					{
						selected.cancel();
						continue;
					}

					try
					{
    					//Handling the connect if possible
						if (selected.isConnectable())
						{
							if (sock.finishConnect())
							{
//								System.out.println("[CONNECT] " + peer.getSockAddr());
								selected.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
							}
							else
							{
								selected.cancel();
								throw new Exception("Unable to connect to the peer");
							}
						}

						//Handling the read if possible
						if (selected.isReadable())
						{
							if((sock.read(peer.getReadBuffer()) > 0) && (peer.getReadBuffer().position() >= 68))
							{
//								System.out.print("[HANDSHAKE] " + peer.getSockAddr());

								//Moving the peer to the main selector if the handshake checks out
								if(peer.checkHandshake() == true)
								{
//									System.out.println("... Success!");
									selected.cancel();
									sock.register(select, SelectionKey.OP_WRITE | SelectionKey.OP_READ);

									//Clearing the current queued messages and sending our bitfield if we have pieces
									if(receivedPieces.cardinality() != 0)
									{
										peer.getWriteMsgBuffer().clear();

										int totalPieces = info.getTotalPieces();
										ByteBuffer payload = ByteBuffer.allocate((int)Math.ceil((double)totalPieces / 8));

										for(int p = 0; p < totalPieces;)
										{
											byte temp = 0;

											for(int b = 7; (b >= 0) && (p < totalPieces); b--)
											{
												if(receivedPieces.get(p++))
												{
													temp |= (1 << b);
												}
											}

											payload.put(temp);
										}

										peer.writeMessage(Peer.MessageType.BITFIELD, payload);
									}

									//Handling any messages (bitfield/haves) that were sent at the same time as the handshake
									peer.handleMessages();
								}
								//Removing the peer if the handshake does not check out
								else
								{
//									System.out.println("... FAILED!");
									selected.cancel();
									peers.remove(sock);
								}

								continue;
							}
						}

						//Handling the write if possible
						if (selected.isWritable())
						{
							peer.getWriteBuffer().flip();
							if(peer.getWriteBuffer().hasRemaining())
							{
								if(sock.write(peer.getWriteBuffer()) > 0)
								{
//									System.out.println("[HANDSHAKE SENT] " + peer.getSockAddr());
								}
							}
							peer.getWriteBuffer().compact();
						}
					}
					//Removing the peer due to exception
					catch (Exception e)
					{
//						System.out.println("Handshake Force Remove: " + e);
//						e.printStackTrace();
						forceRemovePeer(peer);
					}
				}
			}
			catch (Exception e)
			{
//				System.out.println("Exception: " + e);
//				e.printStackTrace();
			}

			//Main Selector
			try
			{
				//Performing the select
				//select.selectNow();
				select.select(100);
				Iterator it = select.selectedKeys().iterator();

				while(it.hasNext())
				{
					SelectionKey selected = (SelectionKey)it.next();
					it.remove();

					//Getting the peer associated with the socket channel
					SocketChannel sock = (SocketChannel)selected.channel();
					Peer peer;

					synchronized(peers)
					{
						peer = peers.get(sock);
					}

					//Peer does not exist, so remove the entry from the selector
					if (peer == null)
					{
						selected.cancel();
						continue;
					}

					try
					{
						//Handling the read if possible
						if (selected.isReadable())
						{
							if(sock.read(peer.getReadBuffer()) > 0)
							{
								peer.handleMessages();
							}
						}

						//Handling the write if possible
						if (selected.isWritable())
						{
							peer.setupWrites();

							peer.getWriteBuffer().flip();
							if(peer.getWriteBuffer().hasRemaining())
							{
								if(sock.write(peer.getWriteBuffer()) > 0)
								{
								}
							}
							peer.getWriteBuffer().compact();
						}
					}
					//Removing the peer due to exception
					catch (Exception e)
					{
//						System.out.println("Force Remove: " + e);
//						e.printStackTrace();
						forceRemovePeer(peer);
					}
				}
			}
			catch (Exception e)
			{
//				System.out.println("Exception: " + e);
//				e.printStackTrace();
			}
		}
	}

	/**
		Forces the removal of a peer due to connection issues, etc. Returns all of their requests to the pool
		@param peer peer to remove
	*/
	public void forceRemovePeer(Peer peer)
	{
		if (peer == null)
		{
			return;
		}

		synchronized(peers)
		{
			if (!peers.containsValue(peer))
			{
				return;
			}

//			System.out.println("Removing Peer " + peer.getSockAddr());

			//Adding all the requests to the torrent request pool
			addRequestsToPool(peer.getRequests());
			addRequestsToPool(peer.getSentRequests());

			//Removing the peer from its selector (if necessary)
			for(SelectionKey handshakeSelectKey : handshakeSelect.keys())
			{
				SocketChannel sock = (SocketChannel)handshakeSelectKey.channel();
				if (peer.getSocket().equals(sock))
				{
					handshakeSelectKey.cancel();
					break;
				}
			}
			for(SelectionKey selectKey : select.keys())
			{
				SocketChannel sock = (SocketChannel)selectKey.channel();
				if (peer.getSocket().equals(sock))
				{
					selectKey.cancel();
					break;
				}
			}

			//Removing the peer from the hashmap
			peers.remove(peer.getSocket());
		}
	}

	/**
		Attempts to connect to the peer, set up a handshake message, and add it to the appropriate selector
		@param peer The peer to add
	*/
	public void addPeer(Peer peer, boolean incoming) throws Exception
	{
		synchronized (peers)
		{
			//Dropping connection if torrent has more than 50 peers
			if (peers.values().size() >= 75)
			{
				if (incoming)
				{
					peer.getSocket().close();
				}

				return;
			}

			//Making sure that the peer is not already in the list
			if (!peers.containsValue(peer))
			{
				try
				{
					//Connect to the peer via TCP
					peer.connect();

					//Sending the handshake message to the peer
					peer.getWriteBuffer().put((byte)19).put(protocolName).putDouble(0).put(info.getInfoHash()).put(peerID);

					//Incoming Peer: Already received a valid handshake, so place in main selector
					if (incoming)
					{
//						System.out.println("[PEER INC] " + peer.getSockAddr());
						peer.getSocket().register(select, SelectionKey.OP_WRITE | SelectionKey.OP_READ);

						//Sending our bitfield if we have pieces
						if(receivedPieces.cardinality() != 0)
						{
							int totalPieces = info.getTotalPieces();
							ByteBuffer payload = ByteBuffer.allocate((int)Math.ceil((double)totalPieces / 8));

							for(int p = 0; p < totalPieces;)
							{
								byte temp = 0;

								for(int b = 7; (b >= 0) && (p < totalPieces); b--)
								{
									if(receivedPieces.get(p++))
									{
										temp |= (1 << b);
									}
								}

								payload.put(temp);
							}

							peer.writeMessage(Peer.MessageType.BITFIELD, payload);
						}
					}
					//Outgoing Peer: Waiting on connection and a valid handshake, so place in handshake selector
					else
					{
//						System.out.println("[PEER OUT] " + peer.getSockAddr());
						peer.getSocket().register(handshakeSelect, SelectionKey.OP_CONNECT);
					}
				}
				catch (Exception e)
				{
//					System.out.println("EXCEPTION: " + e);
					return;
				}

				peers.put(peer.getSocket(), peer);
			}
		}
	}

	private class DHTAnnouncer extends TimerTask
	{
		private Torrent toAnnounce;

		public DHTAnnouncer(Torrent toAnnounce)
		{
			this.toAnnounce = toAnnounce;
		}

		/**
			Schedules another announce to occur after a certain number of seconds

			@param seconds Number of seconds before next ammounce
		*/
		private void schedule(int seconds)
		{
			DHTAnnouncerTimer = new Timer(false);
			DHTAnnouncerTimer.schedule(new DHTAnnouncer(toAnnounce), seconds * 1000);
		}

		/**
			Attempts to contact trackers in the announce URL list in order. Upon a successful response, it
			parses it and handles new peer information.

			NOTE: Always schedules itself to run again after a certain number of seconds.
		*/
		public void run()
		{
			try
			{
				byte[] payload = Arrays.copyOf(InetAddress.getLocalHost().getAddress(), 8);
				payload[4] = (byte)(port >> 8);
				payload[5] = (byte)(port);
				payload[6] = (byte)(INITIAL_TTL >> 8);
				payload[7] = (byte)INITIAL_TTL;

				chord.put(new ChordData(info.getInfoHash(), payload), true);

				schedule(DHT_ANNOUNCE_TIMER_PERIOD);
			}
			catch (Exception e)
			{
				System.out.println("Announce failed, will retry in 30 seconds");
				schedule(30);
				return;
			}

			//Getting the peer data from the ring
			RaptorData peerData;

			try
			{
				ChordData data = chord.get(info.getInfoHash());
				peerData = new RaptorData(data.getHash(), data.getData());
			}
			catch (Exception e)
			{
				System.out.println("Retreiving peers failed, will retry in 30 seconds");
				schedule(30);
				return;
			}

			//Saving and shuffling the list of peers
			peerList = peerData.getPeerList();
			Collections.shuffle(peerList);

			//Removing ourselves from the peer list
			try
			{
				peerList.remove(peerData.new PeerData(InetAddress.getLocalHost(), (short)port));
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}

			//Adding peers as necessary
			for (RaptorData.PeerData c : peerList)
			{
			 	try
				{
					addPeer(new Peer(toAnnounce, new byte[20], c.getIPAddress().getHostAddress(), ((int)c.getPort()) & 0xFFFF), false);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	private class UploadSlotAssigner extends TimerTask
	{
		private int slots;

		public UploadSlotAssigner(int slots)
		{
			this.slots = slots;
		}

		/**
			Looks at upload and download totals for each peer and decides the upload slots based on that.
			Assigns one random peer an upload slot regardless of download total.
		*/
		public void run()
		{
			LinkedList<Peer> peerList = getPeers();
			Collections.sort(peerList, new Comparator<Peer>()
			{
				public int compare(Peer a, Peer b)
				{
					int deltaA = a.getDownloaded();
					int deltaB = b.getDownloaded();

					return new Integer(deltaB).compareTo(deltaA);
				}
			});

			//Calculating and reporting upload/download totals
			int downloadedTotal = 0;
			int uploadedTotal = 0;
			for (Peer peer : peerList)
			{
				downloadedTotal += peer.getDownloaded();
				uploadedTotal += peer.getUploaded();
				peer.resetDownloaded();
				peer.resetUploaded();
			}

			if(state == State.COMPLETED)
			{
				System.out.printf("DL %10.2f KBps - UP %10.2f KBps - Completed %10.2f%%\n", ((double)downloadedTotal / 1024.0) / (SEEDING_SLOT_ASSIGN_TIMER_PERIOD / 1000), ((double)uploadedTotal / 1024.0) / (SEEDING_SLOT_ASSIGN_TIMER_PERIOD / 1000), ((double)getReceivedPieces().cardinality() / (double)(getInfo().getPieces().length / 20)) * 100);
			}
			else
			{
				System.out.printf("DL %10.2f KBps - UP %10.2f KBps - Completed %10.2f%%\n", ((double)downloadedTotal / 1024.0) / (SLOT_ASSIGN_TIMER_PERIOD / 1000), ((double)uploadedTotal / 1024.0) / (SLOT_ASSIGN_TIMER_PERIOD / 1000), ((double)getReceivedPieces().cardinality() / (double)(getInfo().getPieces().length / 20)) * 100);
			}

			synchronized(uploadSlotActions)
			{
				LinkedList<Peer> leechers = new LinkedList<Peer>();

				int numSeeders = 0;

				//Choking all seeders and non-interested peers
				for (Peer p : peerList)
				{
					if (p.getPieces().cardinality() == info.getTotalPieces())
					{
						uploadSlotActions.put(p, true);
						numSeeders++;
					}
					else if (!p.isPeerInterested())
					{
						uploadSlotActions.put(p, true);
					}
					else
					{
						leechers.add(p);
					}
				}

//				System.out.println("Seeders: " + numSeeders + " Leechers: " + (peerList.size() - numSeeders));
//				System.out.println("Total Interested: " + leechers.size());

				//Unchoke peers up to (slots - 1) total
				int numUnchoke = Math.min(slots - 1, leechers.size());
				for (int c = 0; c < numUnchoke; c++)
				{
					uploadSlotActions.put(leechers.remove(), false);
				}

				//Unchoke random peer to fill the final upload slot
				if (leechers.size() > 0)
				{
					Collections.shuffle(leechers);
					uploadSlotActions.put(leechers.remove(), false);
				}

				//Choke the rest
				if (leechers.size() > 0)
				{
					for(Peer peer : leechers)
					{
						uploadSlotActions.put(peer, true);
					}
				}
			}
		}
	}

	private class RequestTimer extends TimerTask
	{
		/**
			Sets the request timer status to true
		*/
		public void run()
		{
			requestTimerStatus = true;
		}
	}

	private class EndGameRequestTimer extends TimerTask
	{
		/**
			Sets the end game request timer status to true
		*/
		public void run()
		{
			endGameRequestTimerStatus = true;
		}
	}

	private class ExpirePeersTimer extends TimerTask
	{
		public void run()
		{
			Collection<ChordData> coll = chord.getLocalData();
			if(coll.size() == 0)
				return;

			for (ChordData c : coll)
			{
				RaptorData rawr = null;
				try
				{
					rawr = new RaptorData(c.getHash(), c.getData());
				}
				catch (Exception e)
				{
					System.out.println("length was not multiple of 8");
					return;
				}

				rawr.agePeers();
				rawr.expirePeers();
				c.setData(rawr.getData());
			}

			chord.removeGarbage();
		}
	}
}