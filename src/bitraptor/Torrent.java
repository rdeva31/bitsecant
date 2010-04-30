package bitraptor;

import java.util.*;
import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import org.klomp.snark.bencode.*;

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
	private HashMap<SocketChannel, Peer> peers;
	private HashMap<Integer, Piece> pieces;
	private HashMap<Peer, Boolean> uploadSlotActions;
	private BitSet receivedPieces;
	private BitSet requestedPieces;
	private LinkedList<Request> requestPool;
	private State state = State.STARTED;
	private boolean inEndGameMode;

	private Timer announcerTimer;

	private final int SEEDING_SLOT_ASSIGN_TIMER_PERIOD = 30*1000; //in milliseconds
	private final int SLOT_ASSIGN_TIMER_PERIOD = 10*1000; //in milliseconds
	private final int NUM_UPLOAD_SLOTS = 5; //number of upload slots
	private final int BLOCK_SIZE = 16*1024;
	
	private final int REQUEST_TIMER_PERIOD = 2*1000; //in milliseconds
	private boolean requestTimerStatus = false; 
	
	private final int END_GAME_REQUEST_TIMER_PERIOD = 30*1000; //in milliseconds
	private boolean endGameRequestTimerStatus = false; 
	
	/**
		Initializes the Torrent based on the information from the file.
		@param info Contains torrent characteristics
		@param port Port number between 1024-65535
	*/
	public Torrent(Info info, int port)
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
				
		//Starting up the announcer (runs immediately and then it internally handles scheduling)
		(new TorrentAnnouncer(this)).run();
		
		//Starting up the upload slot assign timer
		Timer uploadSlotTimer = new Timer(false);
		uploadSlotTimer.scheduleAtFixedRate(new UploadSlotAssigner(NUM_UPLOAD_SLOTS), 0, SLOT_ASSIGN_TIMER_PERIOD);
		
		//Starting up the request timer
		Timer requestTimer = new Timer(false);
		requestTimer.scheduleAtFixedRate(new RequestTimer(), 0, REQUEST_TIMER_PERIOD);

		//Initializing the end game request timer
		Timer endGameRequestTimer = null;
		
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
				requestTimer.cancel();
				if (endGameRequestTimer != null)
				{
					endGameRequestTimer.cancel();
				}

				//Setting the upload slot timer to instead run every 30 seconds
				uploadSlotTimer.cancel();
				uploadSlotTimer = new Timer(false);
				uploadSlotTimer.scheduleAtFixedRate(new UploadSlotAssigner(NUM_UPLOAD_SLOTS), 0, SEEDING_SLOT_ASSIGN_TIMER_PERIOD);

				//Canceling the current announcer timer, and doing it immediately to show completition
				announcerTimer.cancel();
				announcerTimer = new Timer(false);
				announcerTimer.schedule(new TorrentAnnouncer(this), 0);

				break;
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
			//Removing the peer due to exception
			catch (Exception e)
			{
//				System.out.println("Exception: " + e);
//				e.printStackTrace();
				return;
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
			//Removing the peer due to exception
			catch (Exception e)
			{
//				System.out.println("Exception: " + e);
//				e.printStackTrace();
				return;
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
			if (peers.values().size() >= 50)
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
					peer.getWriteBuffer().put((byte)19).put(protocolName).putDouble(0.0).put(info.getInfoHash()).put(peerID);

					//Incoming Peer: Already received a valid handshake, so place in main selector
					if (incoming)
					{
//						System.out.println("[PEER INC] " + peer.getSockAddr());
						peer.getSocket().register(select, SelectionKey.OP_WRITE | SelectionKey.OP_READ);
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

				//Adding the peer to the list
				peers.put(peer.getSocket(), peer);
			}
		}
	}
	
	private class TorrentAnnouncer extends TimerTask
	{
		private Torrent toAnnounce;
		
		public TorrentAnnouncer(Torrent toAnnounce)
		{
			this.toAnnounce = toAnnounce;
		}
	
		/**
			Encodes an array of bytes into a valid URL string representation
		
			@param data Array of bytes
		
			@return Encoded representation String
		*/
		private String encode(byte[] data)
		{
			String encoded = "";
		
			for (int b = 0; b < data.length; b++)
			{
				encoded += "%" + (((data[b] & 0xF0) == 0) ? ("0" + Integer.toHexString(data[b] & 0xFF)) : Integer.toHexString(data[b] & 0xFF));
			}
		
			return encoded;
		}
		
		/**
			Schedules another announce to occur after a certain number of seconds
		
			@param seconds Number of seconds before next ammounce
		*/
		private void schedule(int seconds)
		{
			announcerTimer = new Timer(false);
			announcerTimer.schedule(new TorrentAnnouncer(toAnnounce), seconds * 1000);
		}
		
		/**
			Attempts to contact trackers in the announce URL list in order. Upon a successful response, it
			parses it and handles new peer information.
			
			NOTE: Always schedules itself to run again after a certain number of seconds.
		*/
		public void run()
		{
			byte[] response = null;
		
			//Going through all the announce URLs (if needed)
			for (URL announceURL : info.getAnnounceUrls())
      		{
				//Setting up the query URL
				String query = "?info_hash=" + encode(info.getInfoHash()) + "&peer_id=" + encode(peerID) + "&port=" + port + 
				"&uploaded=0&downloaded=0&left=" + info.getFileLength() + "&compact=0&no_peer_id=0";
				
				//Including event if not in RUNNING state
				if (state != State.RUNNING)
				{
					query += "&event=" + state.toString().toLowerCase();
				}
			
				//Including tracker ID if it was set by the tracker previously
				if (trackerID != null)
				{
					query += "&trackerid=" + trackerID;
				}
		
				try
				{
//					System.out.println("[ANNOUNCE] " + announceURL.toString());
					
					//Initializing the connection
					URL trackerQueryURL = new URL(announceURL.toString() + query);
					HttpURLConnection conn = (HttpURLConnection)(trackerQueryURL.openConnection());
					conn.setRequestMethod("GET");
					conn.setDoOutput(true);
					conn.connect();
		
					//Reading the response from the tracker
					InputStream istream = conn.getInputStream();
					response = new byte[256];
					int totalBytesRead = 0;
					int bytesRead = 0;
				
					while ((bytesRead = istream.read(response, totalBytesRead, 256)) != -1)
					{
						totalBytesRead += bytesRead;
						
						//Done reading, so remove extra bytes from end of response
						if (bytesRead < 256)
						{
							response = Arrays.copyOf(response, totalBytesRead);
						}
						//Set up response for next read
						else
						{
							response = Arrays.copyOf(response, totalBytesRead + 256);
						}
					}

					//Disconnecting from the tracker
					istream.close();
					conn.disconnect();

					break;
				}
				//Move onto the next announce URL
				catch (Exception e)
				{
					continue;
				}
			}
			
			//No response from any of the announce URLs
			if (response == null)
			{
//				System.out.println("ERROR: Couldn't announce");
//				System.out.println("Will retry in 30 seconds...");
				schedule(30);
				return;
			}

			//Updating state to running upon a successful response after started state
			if (state == State.STARTED)
			{
				state = State.RUNNING;
			}
			
			//Parsing the response from the tracker
			try
			{
				BDecoder decoder = new BDecoder(new ByteArrayInputStream(response));
				Map<String, BEValue> replyDictionary = decoder.bdecode().getMap();
				
				//Announce failed
				if (replyDictionary.containsKey("failure reason"))
				{
					String reason = new String(replyDictionary.get("failure reason").getBytes());
//					System.out.println("Announce Failed: " + reason);
					
//					System.out.println("Will retry in 30 seconds...");
					schedule(30);
					return;
				}
				
				int interval = replyDictionary.get("interval").getInt();
				int seeders = replyDictionary.get("complete").getInt();
				int leechers = replyDictionary.get("incomplete").getInt();
				
//				System.out.println("Seeders: " + seeders);
//				System.out.println("Leechers: " + leechers);
				
				//Tracker ID is an optional field
				if (replyDictionary.containsKey("tracker id"))
				{
					trackerID = new String(replyDictionary.get("tracker id").getBytes());
				}

				//Skipping over new peers if we are completed
				if (state != state.COMPLETED)
				{
					//Getting peer information via dictionaries (Throws exception if tracker sent in binary format)
					try
					{
						List<BEValue> peersDictionaries = replyDictionary.get("peers").getList();

						for (BEValue peerDictionary : peersDictionaries)
						{
							Map<String, BEValue> peerDictionaryMap = peerDictionary.getMap();

							byte[] peerID = peerDictionaryMap.get("peer id").getBytes();
							String IPAddr = peerDictionaryMap.get("ip").getString();
							int port = peerDictionaryMap.get("port").getInt();

							addPeer(new Peer(toAnnounce, peerID, IPAddr, port), false);
						}
					}
					//Getting peer information via binary format
					catch (InvalidBEncodingException e)
					{
						byte[] peers = replyDictionary.get("peers").getBytes();

						for (int c = 0; c < peers.length; c += 6)
						{
							String IPAddr = Integer.toString((int)peers[c] & 0xFF) + "."
								+ Integer.toString((int)peers[c + 1] & 0xFF) + "."
								+ Integer.toString((int)peers[c + 2] & 0xFF) + "."
								+ Integer.toString((int)peers[c + 3] & 0xFF);
							int port = (((peers[c + 4] & 0xFF) << 8) + (peers[c + 5] & 0xFF)) & 0xFFFF;

							addPeer(new Peer(toAnnounce, new byte[20], IPAddr, port), false);
						}
					}
				}
				
				//Scheduling another announce after the specified time interval
//				System.out.println("Announce Successful! " + interval + " seconds until next announce");
				schedule(interval);
			}
			//Invalid response from the tracker (Could not be parsed)
			catch (Exception e)
			{
//				System.out.println("ERROR: Received an invalid response from the tracker");
//				System.out.println("Will retry in 30 seconds...");
//				e.printStackTrace();
				schedule(30);
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

			int downloadedTotal = 0;
			int uploadedTotal = 0;
			for (Peer peer : peerList)
			{
				downloadedTotal += peer.getDownloaded();
				uploadedTotal += peer.getUploaded();
				peer.resetDownloaded();
				peer.resetUploaded();
			}

			System.out.printf("DL %10.2f KBps - UP %10.2f KBps - Completed %10.2f%%\n", ((double)downloadedTotal / 1024.0) / 10, ((double)uploadedTotal / 1024.0) / 10, ((double)getReceivedPieces().cardinality() / (double)(getInfo().getPieces().length / 20)) * 100);

			synchronized(uploadSlotActions)
			{
				LinkedList<Peer> leechers = new LinkedList<Peer>();

				//Choking all seeders and non-interested peers
				for (Peer p : peerList)
				{
					if (p.getPieces().cardinality() == info.getTotalPieces())
					{
						uploadSlotActions.put(p, true);
					}
					else if (!p.isInterested())
					{
						uploadSlotActions.put(p, true);
					}
					else
					{
						leechers.add(p);
					}
				}

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
}
