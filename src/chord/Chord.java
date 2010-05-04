package chord;

import java.io.*;
import java.net.*;
import java.nio.*;
import net.rudp.*;
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
	private ReliableServerSocket sock;


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
		sock = new ReliableServerSocket(port);
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
					stabilize();
				}
				catch (Exception e)
				{
					System.out.println("Exception [" + port + "] : " + e);
					e.printStackTrace();
				}
			}
		}, STABILIZE_TIMER_DELAY, STABILIZE_TIMER_DELAY);

		//CheckPredecessor timer task
		(new Timer()).scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				checkPredecessor();
			}
		}, CHECK_PREDECESSOR_TIMER_DELAY, CHECK_PREDECESSOR_TIMER_DELAY);

		//FixFingers timer task
		(new Timer()).scheduleAtFixedRate(new TimerTask()
		{
			public void run()
			{
				try
				{
					fixFingers();
				}
				catch (Exception e)
				{
					System.out.println("Exception [" + port + "] : " + e);
					e.printStackTrace();
				}
			}
		}, FIX_FINGERS_TIMER_DELAY, FIX_FINGERS_TIMER_DELAY);

		HashMap <SocketAddress, ReliableSocket> clients = sock.getClients();

		for(ReliableSocket clientSock : clients.values())
		{
			try
			{
				BufferedInputStream in = new BufferedInputStream(clientSock.getInputStream());

				if(in.available() > 0)
				{
					byte[] header = new byte[4];
					in.read(header, 0, 4);

					int messageLength = (((int)header[0] & 0xFF) << 24) + (((int)header[1] & 0xFF) << 16) +
										(((int)header[2] & 0xFF) << 8) + ((int)header[0] & 0xFF);

					byte[] message = new byte[messageLength];
					in.read(message, 0, messageLength);

					final byte[] messageCopy = message;
					final ReliableServerSocket sockCopy = sock;
					final ReliableSocket clientSockCopy = clientSock;

					new Thread(new Runnable()
					{
						public void run()
						{
							try
							{
								handleMessage(ByteBuffer.wrap(messageCopy), new ChordNode(clientSockCopy));
							}
							catch (Exception e)
							{
								sockCopy.removeClientSocket(clientSockCopy._endpoint);
							}
						}
					}).start();
				}
			}
			catch (Exception e)
			{
				sock.removeClientSocket(clientSock._endpoint);
			}
		}
	}

	public void create() throws Exception
	{
		predecessor = null;
		successor = key;
	}

	public void join(ChordNode node) throws Exception
	{
		predecessor = null;

		ByteBuffer buffer = ByteBuffer.allocate(20);
		buffer.put(key.getHash());

		node.sendMessage(ChordNode.MessageType.SUCCESSOR, buffer);
		buffer = node.getResponse();

		byte[] IPAddress = new byte[4];
		short port;

		buffer.get(IPAddress);
		port = buffer.getShort();

		successor = new ChordNode(InetAddress.getByAddress(IPAddress), port);
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

			ByteBuffer buffer = ByteBuffer.allocate(20);
			buffer.put(hash);

			closest.sendMessage(ChordNode.MessageType.SUCCESSOR, buffer);
			buffer = closest.getResponse();

			byte[] IPAddress = new byte[4];
			short port;

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

		ByteBuffer response;

		successor.sendMessage(ChordNode.MessageType.PREDECESSOR, null);
		response = successor.getResponse();

		byte[] IPAddress = new byte[4];
		short port;

		response.get(IPAddress);
		port = response.getShort();

		ChordNode node = new ChordNode(InetAddress.getByAddress(IPAddress), port);

		if (ChordNode.isInRange(node.getHash(), key.getHash(), false, successor.getHash(), false))
		{
			successor = node;
		}
		else
		{
			ByteBuffer buffer = ByteBuffer.allocate(6);
			buffer.put(key.getIPAddress().getAddress());
			buffer.putShort(key.getPort());

			successor.sendMessage(ChordNode.MessageType.NOTIFY, buffer);
		}
	}

	private void notify(ChordNode node)
	{
		if ((predecessor == null) || ChordNode.isInRange(node.getHash(), predecessor.getHash(), false, key.getHash(), false))
		{
			predecessor = node;
		}
	}


	private void fixFingers() throws Exception
	{
		for (int i = 1; i < FINGER_TABLE_SIZE; ++i)
		{
			BigInteger temp = new BigInteger("2").pow(i-1).add(new BigInteger(key.getHash())); //hash + 2^next-1
			fingerTable.add(i, findSuccessor(temp.toByteArray()));
		}
	}

	private void checkPredecessor()
	{
		try
		{
			ByteBuffer buf = ByteBuffer.allocate(6);
			buf.put(key.getIPAddress().getAddress());
			buf.putShort((short)key.getPort());

			predecessor.sendMessage(ChordNode.MessageType.PING, buf);
			predecessor.getResponse();
		}
		catch (Exception e)
		{
			predecessor = null;
		}
	}

	private void handleMessage(ByteBuffer buffer, ChordNode node) throws Exception
	{
		ChordNode.MessageType messageType = ChordNode.MessageType.fromInt((int)buffer.getChar());
		switch (messageType)
		{
			case PING:
			{
				node.sendMessage(ChordNode.MessageType.PING_REPLY, null);
				break;
			}
			case SUCCESSOR:
			{
				byte[] hash = new byte[20];
				buffer.get(hash);
				ChordNode successor = findSuccessor(hash);

				ByteBuffer response = ByteBuffer.allocate(6);
				response.put(successor.getIPAddress().getAddress());
				response.putShort((short)successor.getPort());

				node.sendMessage(ChordNode.MessageType.SUCCESSOR_REPLY, response);


				break;
			}
			case PREDECESSOR:
			{
				ByteBuffer response = ByteBuffer.allocate(6);
				response.put(predecessor.getIPAddress().getAddress());
				response.putShort((short)predecessor.getPort());

				node.sendMessage(ChordNode.MessageType.PREDECESSOR_REPLY, response);

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

				node.sendMessage(ChordNode.MessageType.GET_REPLY, response);


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

		node.close();
	}
}