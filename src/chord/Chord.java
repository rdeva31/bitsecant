package chord;

public class Chord
{
	private final int FINGER_TABLE_SIZE = 160;
	private final int TIMER_DELAY = 1*1000; //milliseconds
	
	private Map<String, ChordData> data;
	private List<ChordNode> fingerTable;
	private ChordNode predecessor, successor,  key;
	private ServerSocketChannel sock;
	private Selector select;
	
	public Chord(int port) throws Exception
	{
		key = new ChordNode(InetAddress.getLocalHost(), port);
		predecessor = null;
		sucessor = key;
		
		fingerTable = new ArrayList<ChordNode>(FINGER_TABLE_SIZE);
		data = new HashMap<String, ChordData>();
		
		//Setting up selectors
		select = Selector.open();
		
		//Setting up the listening socket server
		sock = ServerSocketChannel.open();
		sock.configureBlocking(false);
		sock.socket().setReuseAddress(true);
		sock.socket().bind(new InetSocketAddress(port));
		
		sock.register(select, SelectionKey.OP_ACCEPT);
	}
	
	public void listen()
	{
		new Timer().scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				stabilize();
				fixFingers();
				checkPredecessor();
			}
		}, 0, TIMER_DELAY);
		
		try
		{
			select.select(100);
			Iterator it = select.selectedKeys().iterator();
			
			while(it.hasNext())
			{
				SelectionKey selected = (SelectionKey)it.next();
				it.remove();

				//Handling accept
				if(selected.isAcceptable())
				{
					ServerSocketChannel selectedSock = (ServerSocketChannel)selected.channel();
					
					SocketChannel acceptedSock = selectedSock.accept();
					acceptedSock.configureBlocking(true);
					acceptedSock.register(select, SelectionKey.OP_CONNECT);
				}

				//Handling connect
				if(selected.isConnectable())
				{
					SocketChannel selectedSock = (SocketChannel)selected.channel();
					
					if(selectedSock.finishConnect())
					{
						selected.interestOps(SelectionKey.OP_READ);
					}
					else
					{
						selected.cancel();
					}
				}
				
				//Handling read
				if(selected.isReadable())
				{
					SocketChannel selectedSock = (SocketChannel)selected.channel();
					
					ByteBuffer header = ByteBuffer.allocate(4);
					header.order(ByteOrder.BIG_ENDIAN);
		
					selectedSock.read(header);
					header.flip();
		
					int messageLength = header.getInt();
		
					ByteBuffer message = ByteBuffer.allocate(messageLength);
					message.order(ByteOrder.BIG_ENDIAN);
		
					selectedSock.read(message);
					message.flip();
					
					handleMessage(message);
				}
			}
		}
		catch(Exception e)
		{
		}
	}
	
	public void join(ChordNode ringParticipant)
	{
		predecessor = null;
		sucessor = findSuccessor(key.getHash());
	}
	
	private ChordNode findSuccessor(byte[] hash)
	{
		if (key.isBefore(hash) && (successor.isAfter(hash) || successor.isSame(hash)))
		{
			return successor;
		}
		else
		{
			ByteBuffer buffer = ByteBuffer.allocate(20);
			buffer.put(participant.getHash());
			successor.sendMessage(MessageType.SUCCESSOR, buffer);
			
			buffer = successor.getResponse();
			
			if (MessageType.fromInt(buffer.getByte()) != MessageType.SUCCESSOR_REPLY)
			{
				throw new Exception("unexpected response; expected SUCCESSOR_REPLY");
			}
			
			byte[] IPAddress = new byte[4];
			short port;
			
			buffer.getBytes(IPAddress);
			port = buffer.getShort();
			
			return new ChordNode(InetAddress.getByAddress(IPAddress), port);
		}
	}
	
	private void stabilize()
	{
		successor.sendMessage(MessageType.PREDECESSOR, null);
		
		ByteBuffer response = successor.getResponse();
		
		byte[] IPAddress = new byte[4];
		short port;
		
		response.getBytes(IPAddress);
		port = response.getShort();
		
		ChordNode node = new ChordNode(InetAddress.getByAddress(IPAddress), port);
		
		if (node.isAfter(key) && node.isBefore(successor))
		{
			successor = node;
		}
		else
		{
			ByteBuffer buffer = ByteBuffer.allocate(6);
			buffer.put(key.getIPAddress().getAddress());
			buffer.putShort(key.getPort());
			
			successor.sendMessage(MessageType.NOTIFY, buffer);
		}
	}
	
	private void notify(ChordNode n)
	{
		if (predecessor == null || (n.isAfter(predecessor) && n.isBefore(key)))
		{
			predecessor = n;
		}
	}
	
	
	private void fixFingers()
	{
		for (i = 1; i < FINGER_TABLE_SIZE; ++i)
		{
			fingerTable.remove(i);
			BigInteger temp = new BigInteger("2").pow(next-1).add(key.getHash()); //hash + 2^next-1
			fingerTable.add(i, findSucessor(temp.toByteArray()));
		}
	}
	
	private void checkPredecessor()
	{
		try
		{
			predecessor.sendMessage(MessageType.PING, null);
			predecessor.getReponse();
		}
		catch (Exception e)
		{
			predecessor = null;
		}
	}
}