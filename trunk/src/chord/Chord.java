package chord;

import java.io.*;
import java.net.*;
import java.nio.*;
import srudp.*;
import java.util.*;
import java.math.BigInteger;

public class Chord
{
	private final int HASH_SIZE = 20;
	private final int FINGER_TABLE_SIZE = 160;
	private final int STABILIZE_TIMER_DELAY = 5*1000; //milliseconds
	private final int CHECK_PREDECESSOR_TIMER_DELAY = 5*1000; //milliseconds
	private final int FIX_FINGERS_TIMER_DELAY = 100; //milliseconds
	private final int RESILIENCY = 3; 
	private final int SUCCESSOR_LIST_SIZE = 3; 

	private int port;
	private Map<String, ChordData> dataMap;
	private List<ChordNode> fingerTable;
	private List<ChordNode> successorList;
	private int nextFingerToFix;
	private ChordNode predecessor, key;
	private RUDPServerSocket sock;

	public static void main(String[] args)
	{
		try
		{
			new Chord(Integer.parseInt(args[0])).listen();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public Chord(int port) throws Exception
	{
		this.port = port;
		key = new ChordNode(InetAddress.getLocalHost(), (short)port);
		predecessor = null;

		fingerTable = new ArrayList<ChordNode>(FINGER_TABLE_SIZE);

		for (int f = 0; f < FINGER_TABLE_SIZE; f++)
		{
			fingerTable.add(key);
		}

		successorList = new ArrayList<ChordNode>(SUCCESSOR_LIST_SIZE);

		for (int s = 0; s < SUCCESSOR_LIST_SIZE; s++)
		{
			successorList.add(key);
		}

		dataMap = new HashMap<String, ChordData>();
		
		nextFingerToFix = 0;

		//Setting up the listening socket
		sock = new RUDPServerSocket(port);
		
	}

	public ChordNode getKey()
	{
		return key;
	}

	public void listen() throws Exception
	{
		//Stabilize timer task
		(new Timer()).scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				try
				{
					//System.out.println("STABILIZE [" + port + "]");
					stabilize();
				}
				catch (Exception e)
				{
					System.out.println("Exception [" + port + "] : " + e);
					e.printStackTrace();
				}
			}
		}, 1000, STABILIZE_TIMER_DELAY);

		//CheckPredecessor timer task
		(new Timer()).scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				checkPredecessor();
			}
		}, 1000, CHECK_PREDECESSOR_TIMER_DELAY);

		//FixFingers timer task
		(new Timer()).scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				try
				{
					//System.out.println("FIXFINGERS [" + port + "]");
					fixFingers();
				}
				catch (Exception e)
				{
					System.out.println("Exception [" + port + "] : " + e);
					e.printStackTrace();
				}
			}
		}, 1000, FIX_FINGERS_TIMER_DELAY);

		ByteBuffer readBuffer = ByteBuffer.allocateDirect(64*1024);
		
		while(true)
		{
			readBuffer.clear();
			final RUDPSocket client = sock.read(readBuffer);

			final ByteBuffer message = ByteBuffer.allocate(readBuffer.remaining());
			message.order(ByteOrder.BIG_ENDIAN);
			message.put(readBuffer);
			message.flip();
			
			byte temp = message.get();
			//System.out.println("MESSAGE [" + port + "] : " + temp);
			message.position(0);

			new Thread(new Runnable()
			{
				public void run()
				{
					try
					{
						handleMessage(message, new ChordNode(client));
					}
					catch (Exception e)
					{
						System.out.println("Exception [" + port + "] : " + e);
						e.printStackTrace();
					}
				}
			}).start();
		}
	}

	public void create() throws Exception
	{
		predecessor = null;

		System.out.println("CREATE [" + port + "] - Hash " + key.getHashString());
	}

	public byte[] get(byte[] hash) throws Exception
	{
		ChordNode node = findSuccessor(hash);
		
		ByteBuffer message = ByteBuffer.allocate(HASH_SIZE);
		message.put(hash);
		
		node.connect();
		node.sendMessage(ChordNode.MessageType.GET, message);
		ByteBuffer response = node.getResponse();
		node.close();

		if (ChordNode.MessageType.fromInt((int)response.get()) != ChordNode.MessageType.GET_REPLY)
			throw new Exception("expected GET_REPLY");

		byte[] toReturn = new byte[response.remaining()];
		response.get(toReturn);
		return toReturn;
	}

	public void put(byte[] hash, byte[] data, boolean append) throws Exception
	{
		
		ChordNode node = findSuccessor(hash);
		node.connect();
		ByteBuffer toSend = ByteBuffer.allocate(HASH_SIZE + data.length);
		toSend.put(hash);
		toSend.put(data);
		node.sendMessage(append ? ChordNode.MessageType.APPEND : ChordNode.MessageType.PUT, toSend);
		node.close();
		
		//add resiliency
		ChordNode prevNode = findSuccessor(node.getHash());
		for (int c = 0; c < RESILIENCY; ++c)
		{
			prevNode.connect();
			prevNode.sendMessage(append ? ChordNode.MessageType.APPEND : ChordNode.MessageType.PUT, toSend);
			prevNode.close();
			
			prevNode = findSuccessor(prevNode.getHash());
		}

	}

	public void join(ChordNode node) throws Exception
	{
		predecessor = null;
		
		System.out.println("START JOIN [" + port + "]");

		ByteBuffer buffer = ByteBuffer.allocate(HASH_SIZE);
		buffer.put(key.getHash());

		node.connect();
		node.sendMessage(ChordNode.MessageType.SUCCESSOR, buffer);
		buffer = node.getResponse();
		node.close();

		byte[] IPAddress = new byte[4];
		short port;

		buffer.order(ByteOrder.BIG_ENDIAN);
		buffer.get(); //waste the message ID
		buffer.get(IPAddress);
		port = buffer.getShort();

		fingerTable.set(0, new ChordNode(InetAddress.getByAddress(IPAddress), port));
		
		System.out.println("JOIN [" + this.port + "] - Hash " + key.getHashString() + " - Found successor [" + port + "]");
	}

	private ChordNode findSuccessor(byte[] hash) throws Exception
	{
		if (ChordNode.isInRange(hash, key.getHash(), false, fingerTable.get(0).getHash(), true))
		{
			return fingerTable.get(0);
		}
		else
		{
			ChordNode closest = findClosestNode(hash);
			
			if(closest.equals(key))
			{
				return fingerTable.get(0);
			}

			ByteBuffer buffer = ByteBuffer.allocate(HASH_SIZE);
			buffer.put(hash);

			closest.connect();
			closest.sendMessage(ChordNode.MessageType.SUCCESSOR, buffer);
			buffer = closest.getResponse();
			closest.close();

			byte[] IPAddress = new byte[4];
			short port;

			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.get(); //waste the message ID
			buffer.get(IPAddress);
			port = buffer.getShort();

			return new ChordNode(InetAddress.getByAddress(IPAddress), port);
		}
	}

	private ChordNode findClosestNode(byte[] hash)
	{
		for (int i = (FINGER_TABLE_SIZE - 1); i >= 0; i--)
		{
			ChordNode temp = fingerTable.get(i);
			if (ChordNode.isInRange(temp.getHash(), key.getHash(), false, hash, false))
			{
				return temp;
			}
		}

		return key;
	}

	private void stabilize() throws Exception
	{
		if (fingerTable.get(0) == null)
		{
			return;
		}

		fingerTable.get(0).connect();
		fingerTable.get(0).sendMessage(ChordNode.MessageType.PREDECESSOR, null);
		ByteBuffer response = fingerTable.get(0).getResponse();
		fingerTable.get(0).close();
		
		byte[] IPAddress = new byte[4];
		short port;

		response.order(ByteOrder.BIG_ENDIAN);
		response.get(); //waste the message ID
		response.get(IPAddress);
		port = response.getShort();
		
		//Predecessor was null for our successor
		if((IPAddress[0] | IPAddress[1] | IPAddress[2] | IPAddress[3]) == 0)
		{
			ByteBuffer buffer = ByteBuffer.allocate(6);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.put(key.getIPAddress().getAddress());
			buffer.putShort(key.getPort());

			fingerTable.get(0).connect();
			fingerTable.get(0).sendMessage(ChordNode.MessageType.NOTIFY, buffer);
			fingerTable.get(0).close();
			
			return;
		}
		
		ChordNode node = new ChordNode(InetAddress.getByAddress(IPAddress), port);

		if (ChordNode.isInRange(node.getHash(), key.getHash(), false, fingerTable.get(0).getHash(), false))
		{
			fingerTable.set(0, node);
		}
		else
		{
			ByteBuffer buffer = ByteBuffer.allocate(6);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.put(key.getIPAddress().getAddress());
			buffer.putShort(key.getPort());

			fingerTable.get(0).connect();
			fingerTable.get(0).sendMessage(ChordNode.MessageType.NOTIFY, buffer);
			fingerTable.get(0).close();
		}

	}

	private void notify(ChordNode node) throws Exception
	{
		//System.out.println("NOTIFY [" + port + " from " + node.getPort() + "]");
		
		if ((predecessor == null) || ChordNode.isInRange(node.getHash(), predecessor.getHash(), false, key.getHash(), false))
		{
			predecessor = node;
		}
	}


	private void fixFingers() throws Exception
	{
		BigInteger hashNumCeil = new BigInteger("2").pow(160);
		BigInteger nextHashNum = new BigInteger("2").pow(nextFingerToFix).add(new BigInteger(key.getHash())).mod(hashNumCeil);
		byte[] nextHashNumBytes = nextHashNum.toByteArray();
		byte[] nextHash = new byte[HASH_SIZE];
		
		if(nextHashNumBytes.length <= HASH_SIZE)
		{
			System.arraycopy(nextHashNumBytes, 0, nextHash, HASH_SIZE - nextHashNumBytes.length, nextHashNumBytes.length);
		}
		else
		{
			System.arraycopy(nextHashNumBytes, nextHashNumBytes.length - HASH_SIZE, nextHash, 0, HASH_SIZE);
		}

		ChordNode node = findSuccessor(nextHash);
		
		
		if(!node.equals(fingerTable.get(nextFingerToFix)))
		{
			fingerTable.set(nextFingerToFix, node);
		}
		
		nextFingerToFix = (nextFingerToFix + 1) % FINGER_TABLE_SIZE;
	}

	private void checkPredecessor()
	{
		try
		{
			ByteBuffer buffer = ByteBuffer.allocate(6);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.put(key.getIPAddress().getAddress());
			buffer.putShort(key.getPort());

			predecessor.connect();
			predecessor.sendMessage(ChordNode.MessageType.PING, buffer);
			predecessor.getResponse();
			predecessor.close();
		}
		catch (Exception e)
		{
			predecessor = null;
			/*
			LinkedList<ByteBuffer> sendList = new LinkedList<ByteBuffer>();
			
			synchronized(dataMap)
			{
				for(ChordData data : dataMap.values())
				{
					if(ChordNode.compare(data.getHash(), predecessor.getHash()) < 0)
					{
						ByteBuffer toSend = ByteBuffer.allocate(HASH_SIZE + data.getData().length);
						toSend.put(data.getHash());
						toSend.put(data.getData());
						predData.add(toSend);
					}
				}
			}
			
			synchronized(successorList)
			{
				for(ChordNode node : successorList)
				{
					try
					{
						node.connect();
						for(ChordData data : predData)
						{
							node.sendMessage(ChordNode.MessageType.PUT, toSend);
						}
						node.close();
					}
					catch (Exception e)
					{
					}
				}
			}
			*/
		}
	}

	private void handleMessage(ByteBuffer buffer, ChordNode node) throws Exception
	{
		ChordNode.MessageType messageType = ChordNode.MessageType.fromInt((int)buffer.get() & 0xFF);

		switch (messageType)
		{
			case PING:
			{
				node.connect();
				node.sendMessage(ChordNode.MessageType.PING_REPLY, null);
				node.close();
				
				break;
			}
			case SUCCESSOR:
			{
				byte[] hash = new byte[HASH_SIZE];
				buffer.get(hash);
				ChordNode successor = findSuccessor(hash);

				ByteBuffer response = ByteBuffer.allocate(6);
				response.order(ByteOrder.BIG_ENDIAN);
				response.put(successor.getIPAddress().getAddress());
				response.putShort(successor.getPort());

				node.connect();
				node.sendMessage(ChordNode.MessageType.SUCCESSOR_REPLY, response);
				node.close();
				
				break;
			}
			case PREDECESSOR:
			{
				ByteBuffer response = ByteBuffer.allocate(6);
				response.order(ByteOrder.BIG_ENDIAN);
				
				if (predecessor != null)
				{
					response.put(predecessor.getIPAddress().getAddress());
					response.putShort((short)predecessor.getPort());
				}
				else
				{
					byte[] emptyMessage = new byte[6];
					Arrays.fill(emptyMessage, (byte)0);
					response.put(emptyMessage);
				}
				
				node.connect();
				node.sendMessage(ChordNode.MessageType.PREDECESSOR_REPLY, response);
				node.close();

				break;
			}
			case NOTIFY:
			{
				byte[] IPAddress = new byte[4];
				short port;
				
				buffer.get(IPAddress);
				port = buffer.getShort();
				
				ChordNode possiblePredecessor = new ChordNode(InetAddress.getByAddress(IPAddress), port);
				notify(possiblePredecessor);

				break;
			}
			case GET:
			{
				byte[] hash = new byte[HASH_SIZE];
				buffer.get(hash);
				ByteBuffer response = null;
				
				synchronized(dataMap)
				{
					ChordData data = dataMap.get(new String(hash));

					if (data == null)
					{
						throw new Exception("requested data not found");
					}

					response = ByteBuffer.allocate(data.getData().length);
					response.put(data.getData());
				}

				node.connect();
				node.sendMessage(ChordNode.MessageType.GET_REPLY, response);
				node.close();
				
				break;
			}
			case PUT:
			{
				byte[] hash = new byte[HASH_SIZE];
				byte[] data = new byte[buffer.remaining() - HASH_SIZE];
				buffer.get(hash);
				buffer.get(data);

				synchronized(dataMap)
				{
					ChordData existingData = dataMap.get(new String(hash));

					if(existingData == null)
					{
						dataMap.put(new String(hash), new ChordData(hash, data));
					}
					else
					{
						existingData.setData(data);
					}
				}

				break;
			}
			case APPEND:
			{
				byte[] hash = new byte[HASH_SIZE];
				byte[] data = new byte[buffer.remaining() - HASH_SIZE];
				buffer.get(hash);
				buffer.get(data);

				synchronized(dataMap)
				{
					ChordData existingData = dataMap.get(new String(hash));

					if (existingData == null)
					{
						throw new Exception("existing data not found");
					}

					byte[] newData = Arrays.copyOf(existingData.getData(), existingData.getData().length + data.length);
					System.arraycopy(data, 0, newData, existingData.getData().length, data.length);

					existingData.setData(newData);
				}

				break;
			}
			default:
				throw new Exception("unexpected message");
		}
	
	}
}
