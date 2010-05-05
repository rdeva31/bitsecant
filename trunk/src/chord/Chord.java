package chord;

import java.io.*;
import java.net.*;
import java.nio.*;
import srudp.*;
import java.util.*;
import java.math.BigInteger;

public class Chord
{
	private final int FINGER_TABLE_SIZE = 160;
	private final int STABILIZE_TIMER_DELAY = 10*1000; //milliseconds
	private final int CHECK_PREDECESSOR_TIMER_DELAY = 10*1000; //milliseconds
	private final int FIX_FINGERS_TIMER_DELAY = 30*1000; //milliseconds

	private int port;
	private Map<String, ChordData> dataMap;
	private List<ChordNode> fingerTable;
	private ChordNode predecessor, successor, key;
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
		successor = key;

		fingerTable = new ArrayList<ChordNode>(FINGER_TABLE_SIZE);

		for (int i = 0; i < FINGER_TABLE_SIZE; i++)
		{
			fingerTable.add(i, key);
		}

		dataMap = new HashMap<String, ChordData>();

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
					System.out.println("STABILIZE [" + port + "]");
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
					System.out.println("FIXFINGERS [" + port + "]");
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
			System.out.println("MESSAGE [" + port + "] : " + temp);
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
		successor = key;

		System.out.println("CREATE [" + port + "] - Hash " + new String(key.getHash()));
	}

	public void join(ChordNode node) throws Exception
	{
		predecessor = null;
		
		System.out.println("START JOIN [" + port + "]");

		ByteBuffer buffer = ByteBuffer.allocate(20);
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

		successor = new ChordNode(InetAddress.getByAddress(IPAddress), port);
		
		System.out.println("JOIN [" + this.port + "] - Hash " + new String(key.getHash()) + " - Found successor " + InetAddress.getByAddress(IPAddress) + ":" + port);
	}

	private ChordNode findSuccessor(byte[] hash) throws Exception
	{
		if (ChordNode.isInRange(hash, key.getHash(), false, successor.getHash(), true))
		{
			return successor;
		}
		else
		{
			ChordNode closest = findClosestNode(hash);
			
			if(closest.equals(key))
			{
				return successor;
			}

			ByteBuffer buffer = ByteBuffer.allocate(20);
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
		for (int i = (FINGER_TABLE_SIZE - 1); i > 0; i--)
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
		if (successor == null)
		{
			return;
		}

		successor.connect();
		successor.sendMessage(ChordNode.MessageType.PREDECESSOR, null);
		ByteBuffer response = successor.getResponse();
		successor.close();
		
		byte[] IPAddress = new byte[4];
		short port;

		response.order(ByteOrder.BIG_ENDIAN);
		response.get(); //waste the message ID
		response.get(IPAddress);
		port = response.getShort();
		
		//Predecessor was null for our successor
		if((IPAddress[0] | IPAddress[1] | IPAddress[2] | IPAddress[3]) == 0)
		{
			return;
		}
		
		ChordNode node = new ChordNode(InetAddress.getByAddress(IPAddress), port);

		if (ChordNode.isInRange(node.getHash(), key.getHash(), false, successor.getHash(), false))
		{
			successor = node;
		}
		else
		{
			ByteBuffer buffer = ByteBuffer.allocate(6);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.put(key.getIPAddress().getAddress());
			buffer.putShort(key.getPort());

			successor.connect();
			successor.sendMessage(ChordNode.MessageType.NOTIFY, buffer);
			successor.close();
		}
	}

	private void notify(ChordNode node) throws Exception
	{
		if ((predecessor == null) || ChordNode.isInRange(node.getHash(), predecessor.getHash(), false, key.getHash(), false))
		{
			predecessor = node;
		}
	}


	private void fixFingers() throws Exception
	{
		BigInteger hashNumCeil = new BigInteger("2").pow(160);
		
		for (int i = 1; i < FINGER_TABLE_SIZE; ++i)
		{
			BigInteger nextHashNum = new BigInteger("2").pow(i-1).add(new BigInteger(key.getHash())).mod(hashNumCeil);
			byte[] nextHashNumBytes = nextHashNum.toByteArray();
			byte[] nextHash = new byte[20];
			
			if(nextHashNumBytes.length < 20)
			{
				System.arraycopy(nextHashNumBytes, 0, nextHash, 20 - nextHashNumBytes.length, nextHashNumBytes.length);
			}
			else
			{
				System.arraycopy(nextHashNumBytes, 0, nextHash, 0, 20);
			}
			
			ChordNode node = findSuccessor(nextHash);
			
			if(!node.equals(fingerTable.get(i)))
			{
				fingerTable.add(i, node);
			}
		}
	}

	private void checkPredecessor()
	{
		try
		{
			ByteBuffer buffer = ByteBuffer.allocate(6);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.put(key.getIPAddress().getAddress());
			buffer.putShort((short)key.getPort());

			predecessor.connect();
			predecessor.sendMessage(ChordNode.MessageType.PING, buffer);
			predecessor.getResponse();
			predecessor.close();
		}
		catch (Exception e)
		{
			predecessor = null;
		}
	}

	private void handleMessage(ByteBuffer buffer, ChordNode node) throws Exception
	{
		ChordNode.MessageType messageType = ChordNode.MessageType.fromInt((int)buffer.get());

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
				byte[] hash = new byte[20];
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
				byte[] hash = new byte[20];
				buffer.get(hash);

				ChordData data = dataMap.get(new String(hash));

				if (data == null)
				{
					throw new Exception("requested data not found");
				}

				ByteBuffer response = ByteBuffer.allocate(data.getData().length);
				response.put(data.getData());

				node.connect();
				node.sendMessage(ChordNode.MessageType.GET_REPLY, response);
				node.close();
				
				break;
			}
			case PUT:
			{
				byte[] hash = new byte[20];
				byte[] data = new byte[buffer.remaining() - 20];
				buffer.get(hash);
				buffer.get(data);

				ChordData existingData = dataMap.get(new String(hash));

				if(existingData == null)
				{
					dataMap.put(new String(hash), new ChordData(hash, data));
				}
				else
				{
					existingData.setData(data);
				}

				break;
			}
			case ADD:
			{
				byte[] hash = new byte[20];
				byte[] data = new byte[buffer.remaining() - 20];
				buffer.get(hash);
				buffer.get(data);

				ChordData existingData = dataMap.get(new String(hash));

				if (existingData == null)
				{
					throw new Exception("existing data not found");
				}

				byte[] newData = Arrays.copyOf(existingData.getData(), existingData.getData().length + data.length);
				System.arraycopy(data, 0, newData, existingData.getData().length, data.length);

				existingData.setData(newData);

				break;
			}
		}
	
	}
}