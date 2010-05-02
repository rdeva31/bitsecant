package chord;

public class Chord
{
	private final int FINGER_TABLE_SIZE = 160;
	private final int STABILIZE_TIMER_DELAY = 10*1000; //milliseconds
	private final int CHECK_PREDECESSOR_TIMER_DELAY = 10*1000; //milliseconds
	private final int FIX_FINGERS_TIMER_DELAY = 30*1000; //milliseconds
	
	private Map<String, ChordData> dataMap;
	private List<ChordNode> fingerTable;
	private ChordNode predecessor, successor,  key;
	private DatagramChannel sock;
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
		
		//Setting up the listening socket
		sock = DatagramChannel.open();
		sock.socket().bind(new InetSocketAddress(port));
		sock.register(select, SelectionKey.OP_READ);
	}
	
	public void listen()
	{
		//Stabilize timer task
		(new Timer()).scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				stabilize();
			}
		}, 0, STABILIZE_TIMER_DELAY);
		
		//CheckPredecessor timer task
		(new Timer()).scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				checkPredecessor();
			}
		}, 0, CHECK_PREDECESSOR_TIMER_DELAY);
		
		//FixFingers timer task
		(new Timer()).scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				fixFingers();
			}
		}, 0, FIX_FINGERS_TIMER_DELAY);
		
		//Handling incoming connections and messages
		try
		{
			select.select(100);
			Iterator it = select.selectedKeys().iterator();
			
			while(it.hasNext())
			{
				SelectionKey selected = (SelectionKey)it.next();
				it.remove();
				
				//Dropping the socket if it is no longer valid
				if(!selected.isValid())
				{
					selected.cancel();
					continue;
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
					
					new Thread(new Runnable()
					{
						public void run()
						{
							handleMessage(message, new ChordNode(selectedSock));
							selected.cancel();
						}
					}).start();
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Exception: " + e);
			e.printStackTrace();
			return -1;
		}
	}
	
	public void join(ChordNode node)
	{
		predecessor = null;
		
		ByteBuffer buffer = ByteBuffer.allocate(20);
		buffer.put(key.getHash());
		node.sendMessage(MessageType.SUCCESSOR, buffer);
		
		buffer = successor.getResponse();
		
		byte[] IPAddress = new byte[4];
		short port;
		
		buffer.getBytes(IPAddress);
		port = buffer.getShort();
		
		successor = new ChordNode(InetAddress.getByAddress(IPAddress), port);
	}
	
	private ChordNode findSuccessor(byte[] hash)
	{
		if (ChordNode.isInRange(hash, key, false, successor, true))
		{
			return successor;
		}
		else
		{
			ChordNode closest = findClosestNode(hash);
			
			ByteBuffer buffer = ByteBuffer.allocate(20);
			buffer.put(hash);
			closest.sendMessage(MessageType.SUCCESSOR, buffer);
			
			buffer = closest.getResponse();
			
			byte[] IPAddress = new byte[4];
			short port;
			
			buffer.getBytes(IPAddress);
			port = buffer.getShort();
			
			return new ChordNode(InetAddress.getByAddress(IPAddress), port);
		}
	}
	
	private ChordNode findClosestNode(byte[] hash)
	{
		for (i = FINGER_TABLE_SIZE; i > 0; i--)
		{
			ChordNode temp = fingerTable.get(i);
			if (ChordNode.isInRange(temp, key, false, hash, false))
			{
				return temp;
			}
		}
		
		return key;
	}
	
	private void stabilize()
	{
		if (successor == null)
		{
			return;
		}
		
		successor.sendMessage(MessageType.PREDECESSOR, null);
		
		ByteBuffer response = successor.getResponse();
		
		byte[] IPAddress = new byte[4];
		short port;
		
		response.getBytes(IPAddress);
		port = response.getShort();
		
		ChordNode node = new ChordNode(InetAddress.getByAddress(IPAddress), port);
		
		if (ChordNode.isInRange(node, key, false, successor, false))
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
	
	private void notify(ChordNode node)
	{
		if ((predecessor == null) || ChordNode.isInRange(node, predecessor, false, key, false))
		{
			predecessor = node;
		}
	}
	
	
	private void fixFingers()
	{
		for (i = 1; i < FINGER_TABLE_SIZE; ++i)
		{
			fingerTable.remove(i);
			BigInteger temp = new BigInteger("2").pow(next-1).add(key.getHash()); //hash + 2^next-1
			fingerTable.add(i, findSuccessor(temp.toByteArray()));
		}
	}
	
	private void checkPredecessor()
	{
		try
		{
			ByteBuffer buf = ByteBuffer.allocate(6);
			buf.put(key.getIPAddress().getAddress());
			buf.put((short)key.getPort());
			
			predecessor.sendMessage(MessageType.PING, buf);
			predecessor.getReponse();
		}
		catch (Exception e)
		{
			predecessor = null;
		}
	}
	
	private void handleMessage(ByteBuffer buffer, ChordNode node)
	{
		MessageType messageType = MessageType.fromInt((int)buffer.getByte());
		switch (messageType)
		{
			case PING:
			{
				node.sendMessage(MessageType.PING_REPLY, null);
				break;
			}
			case SUCCESSOR:
			{
				byte[] hash = new byte[20];
				buffer.get(hash);
				ChordNode successor = findSucessor(hash);
				
				ByteBuffer response = ByteBuffer.allocate(6);
				response.put(successor.getIPAddress().getAddress());
				response.put((short)successor.getPort());
				
				node.sendMessage(MessageType.SUCCESSOR_REPLY, response);
				
				break;
			}
			case PREDECESSOR:
			{
				ByteBuffer response = ByteBuffer.allocate(6);
				response.put(predecessor.getIPAddress().getAddress());
				response.put((short)predecessor.getPort());
				break;
			}
			case NOTIFY:
			{
				byte[] IPAddress = new byte[4];
				short port;
		
				response.getBytes(IPAddress);
				port = response.getShort();
		
				ChordNode possiblePredecessor = new ChordNode(InetAddress.getByAddress(IPAddress), port);
				notify(possiblePredecessor);
				break;
			}
			case GET:
			{
				byte[] hash = new byte[20];
				byte[] data = null;
				buffer.get(hash);
				Set<String> infoHashSet = dataMap.keySet();
				for (String s : infoHashSet)
				{
					if (s.equals(new String(hash)))
					{
						data = dataMap.get(s);
					}
				}
						
				if (data == null)
				{
					return;
				}
				
				ByteBuffer response = ByteBuffer.allocate(data.length);
				response.put(data);
				
				node.sendMessage(MessageType.GET_REPLY, response);
				break;
			}
			case PUT:
			{
				byte[] hash = new byte[20];
				byte[] data;
				buffer.get(hash);
				data = new byte[buffer.hasRemaining()];
				buffer.get(data);
				
				dataMap.put(new String(hash), new ChordData(hash, data));
				break;
			}
			case ADD:
			{
				byte[] hash = new byte[20];
				byte[] data;
				buffer.get(hash);
				data = new byte[buffer.hasRemaining()];
				buffer.get(data);
				
				byte[] existingData = dataMap.get(new String(hash));
				byte[] newData = Arrays.copyOf(existingData, existingData.length + data.length);
				System.arraycopy(data, 0, newData, existingData.length, data.length);
				
				dataMap.put(new String(hash), newData);
				
				break;
			}
		}
		
		node.disconnect();
	}
}