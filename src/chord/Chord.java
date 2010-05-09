package chord;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.nio.*;
import java.util.*;
import java.util.regex.*;
import java.security.*;
import srudp.*;

public class Chord
{
	private final int HASH_SIZE = 20;
	private final int FINGER_TABLE_SIZE = 160;
	private final int STABILIZE_TIMER_DELAY = 5*1000; //milliseconds
	private final int CHECK_PREDECESSOR_TIMER_DELAY = 5*1000; //milliseconds
	private final int FIX_FINGERS_TIMER_DELAY = 100; //milliseconds
	private final int RESILIENCY = 3; 
	private final int SUCCESSOR_LIST_SIZE = RESILIENCY; 

	private int port;
	private Map<String, ChordData> dataMap;
	private List<ChordNode> fingerTable;
	private LinkedList<ChordNode> successorList;
	private int nextFingerToFix;
	private ChordNode predecessor, key, successor;
	private RUDPServerSocket sock;

	public static void main(String[] args) throws Exception
	{
		int start = 0;
		Chord chord = null;
		short port = 0;
		
		for (start = 0; start < args.length; ++start)
		{
			if (args[start].equalsIgnoreCase("-listen"))
			{
				chord = new Chord((short)Integer.parseInt(args[++start]));
			}
			else if (args[start].equalsIgnoreCase("-join"))
			{
				if (chord == null)
				{
					System.out.println("ERROR: Specify -listen before -join");
					return;
				}
				
				port = (short)Integer.parseInt(args[++start]);
				chord.join(new ChordNode(InetAddress.getLocalHost(), port));
			}
			else if (args[start].equalsIgnoreCase("-create"))
			{
				if (chord == null)
				{
					System.out.println("ERROR: Specify -listen before -create");
					return;
				}
				
				chord.create();
			}
			else
			{
				System.out.println("Usage: <program> -listen <port> [-create] [-join <all other ports>]"); 
				return;
			}
		}
		
		final Chord chordCopy = chord;
		new Thread(new Runnable()
		{
			public void run() 
			{
				try
				{
					chordCopy.listen();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}).start();
				
		Scanner sc = new Scanner(System.in);
		
		while(true)
		{
			String input = sc.nextLine();
			String[] inputs = input.split(" ");
			
			String cmd = inputs[0];
			String[] arguments = null;
			
			if(inputs.length > 1)
			{
				arguments = new String[inputs.length - 1];
				System.arraycopy(inputs, 1, arguments, 0, arguments.length);
			}
			
			if (cmd.equalsIgnoreCase("get"))
			{
				try
				{
					System.out.println("-> GOT: " + new String(chord.get(MessageDigest.getInstance("SHA-1").digest(arguments[0].getBytes()))));
				}
				catch (Exception e)
				{
					System.out.println("-!> GOT: Failed... " + e);
				}
			}
			else if (cmd.equalsIgnoreCase("put"))
			{
				try
				{
					String data = arguments[1];
					
					for(int a = 2; a < arguments.length; a++)
					{
						data += " " + arguments[a];
					}
					
					chord.put(MessageDigest.getInstance("SHA-1").digest(arguments[0].getBytes()), data.getBytes(), false);
					System.out.println("-> PUT: Succeeded!");
				}
				catch (Exception e)
				{
					System.out.println("-!> PUT: Failed... " + e);
				}
			}
			else if (cmd.equalsIgnoreCase("die"))
			{
				System.out.println("-> Euthanizing node in an emergency intelligence incenerator."); 
				System.exit(0);
			}
			else if (cmd.equalsIgnoreCase("print"))
			{
				System.out.println(chord);
			}
			else
				System.out.println("-!>Bad command");
		}
	}

	public Chord(int port) throws Exception
	{
		this.port = port;
		key = new ChordNode(InetAddress.getLocalHost(), (short)port);
		
		predecessor = null;
		
		successor = key;

		fingerTable = new ArrayList<ChordNode>(FINGER_TABLE_SIZE);

		for (int f = 0; f < FINGER_TABLE_SIZE; f++)
		{
			fingerTable.add(key);
		}

		successorList = new LinkedList<ChordNode>();

		for (int s = 0; s < SUCCESSOR_LIST_SIZE; s++)
		{
			successorList.add(key);
		}

		dataMap = new HashMap<String, ChordData>();
		
		nextFingerToFix = 0;

		//Setting up the listening socket
		sock = new RUDPServerSocket(port);
		
	}
	
	public String toString()
	{
		String str = "----------------------------------------------------\n";
		str += "Hash: " + key.getHashString() + "\n";
		str += "----------------------------------------------------\n";
		str += "\tPredecessor:\n";

		if(predecessor != null)
		{
			str += "\t\t" + predecessor.toString() + "\n";
		}
		else
		{
			str += "\t\tNone\n";
		}
		
		str += "\tSuccessors:\n";
		
		synchronized(successorList)
		{
			for(ChordNode node : successorList)
			{
				str += "\t\t" + node.toString() + "\n";
			}
		}
		
		str += "\tData:\n";
		
		synchronized(dataMap)
		{
			for(ChordData data : dataMap.values())
			{
				str += "\t\t" + data.toString() + "\n";
			}
		}
		
		return str;
	}

	public ChordNode getKey()
	{
		return key;
	}
	
	private void setSuccessor(ChordNode successor)
	{
		synchronized(successorList)
		{
			synchronized(fingerTable)
			{
				this.successor = successor;
				successorList.set(0, successor);
				fingerTable.set(0, successor);
			}
		}
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
	}

	public byte[] get(byte[] hash) throws Exception
	{
		ChordNode node = findSuccessor(hash);
	
		ByteBuffer message = ByteBuffer.allocate(HASH_SIZE);
		message.put(hash);
		
		ByteBuffer response = null;
		
		try
		{
			node.connect();
			node.sendMessage(ChordNode.MessageType.GET, message);
			response = node.getResponse();
			node.close();
		}
		catch (Exception e)
		{
			node.close();
			System.out.println("NEED TO IMPLEMENT RESILIENCY FOR GET");
		}
		
		ChordNode.MessageType messageID = ChordNode.MessageType.fromInt((int)response.get() & 0xFF);
		
		if (messageID == ChordNode.MessageType.GET_REPLY)
		{
			byte[] toReturn = new byte[response.remaining()];
			response.get(toReturn);
			return toReturn;
		}
		else if (messageID == ChordNode.MessageType.GET_REPLY_INVALID)
		{
			throw new Exception("Data does not exist");
		}
		else
		{
			throw new Exception("Expected get_reply or get_reply_invalid");
		}
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
	}

	public void join(ChordNode node) throws Exception
	{
		predecessor = null;

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

		setSuccessor(new ChordNode(InetAddress.getByAddress(IPAddress), port));
	}

	private ChordNode findSuccessor(byte[] hash) throws Exception
	{
		if (ChordNode.isInRange(hash, key.getHash(), false, successor.getHash(), true))
		{
			return successor;
		}
		else
		{
			ByteBuffer buffer = ByteBuffer.allocate(HASH_SIZE);
			buffer.put(hash);

			ChordNode closest = findClosestNode(hash);
				
			while(true)
			{
				if(closest.equals(key))
				{
					return successor;
				}
				
				try
				{
					closest.connect();
					closest.sendMessage(ChordNode.MessageType.SUCCESSOR, buffer);
					buffer = closest.getResponse();
					closest.close();
					break;
				}
				catch (Exception e)
				{
					closest.close();
					closest = findClosestNode(closest.getHash());
				}
			}

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
	
	private void stabilizeSuccessorList()
	{
		ByteBuffer response = null;
		
		try
		{
			successor.connect();
			successor.sendMessage(ChordNode.MessageType.SUCCESSOR_LIST, null);
			response = successor.getResponse();
			successor.close();
		}
		catch (Exception e)
		{
			successor.close();
			return;
		}
			
		synchronized(successorList)
		{	
			successorList.clear();
			successorList.add(successor);
			
			response.order(ByteOrder.BIG_ENDIAN);
			response.get(); //waste the message ID
		
			while(response.remaining() > 6) //all but the last node
			{
				byte[] IPAddress = new byte[4];
				short port;

				response.get(IPAddress);
				port = response.getShort();
			
				try
				{
					successorList.add(new ChordNode(InetAddress.getByAddress(IPAddress), port));
				}
				catch (Exception e)
				{
					successorList.add(successorList.getLast());
				}
			}
		}
	}

	private void stabilize() throws Exception
	{
		ByteBuffer response;
		
		while(true)
		{
			try
			{
				successor.connect();
				successor.sendMessage(ChordNode.MessageType.PREDECESSOR, null);
				response = successor.getResponse();
				successor.close();
				break;
			}
			catch (Exception e)
			{
				synchronized(successorList)
				{
					successorList.remove();
					setSuccessor(successorList.get(0));
				}
			}
		}
		
		stabilizeSuccessorList();
		
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

			try
			{
				successor.connect();
				successor.sendMessage(ChordNode.MessageType.NOTIFY, buffer);
				successor.close();
			}
			catch (Exception e)
			{
				successor.close();
			}
			
			return;
		}
		
		ChordNode node = new ChordNode(InetAddress.getByAddress(IPAddress), port);

		if (ChordNode.isInRange(node.getHash(), key.getHash(), false, successor.getHash(), false))
		{
			setSuccessor(node);
			stabilizeSuccessorList();
		}
		else
		{
			ByteBuffer buffer = ByteBuffer.allocate(6);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.put(key.getIPAddress().getAddress());
			buffer.putShort(key.getPort());

			try
			{
				successor.connect();
				successor.sendMessage(ChordNode.MessageType.NOTIFY, buffer);
				successor.close();
			}
			catch (Exception e)
			{
				successor.close();
			}
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
		if(predecessor == null)
		{
			return;
		}
	
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
			predecessor.close();
			
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
						sendList.add(toSend);
					}
				}
			}
			
			ChordNode last;
			synchronized(successorList)
			{
				last = successorList.getLast();
			}
			
			try
			{
				last.connect();
				for(ByteBuffer toSend : sendList)
				{
					last.sendMessage(ChordNode.MessageType.PUT, toSend);
				}
				last.close();
			}
			catch (Exception ex)
			{
				last.close();
			}
		
			predecessor = null;
		}
	}

	private void handleMessage(ByteBuffer buffer, ChordNode node) throws Exception
	{
		ChordNode.MessageType messageType = ChordNode.MessageType.fromInt((int)buffer.get() & 0xFF);

		switch (messageType)
		{
			case PING:
			{
				try
				{
					node.connect();
					node.sendMessage(ChordNode.MessageType.PING_REPLY, null);
					node.close();
				}
				catch (Exception e)
				{
					node.close();
				}
				
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

				try
				{
					node.connect();
					node.sendMessage(ChordNode.MessageType.SUCCESSOR_REPLY, response);
					node.close();
				}
				catch (Exception e)
				{
					node.close();
				}
				
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
				
				try
				{
					node.connect();
					node.sendMessage(ChordNode.MessageType.PREDECESSOR_REPLY, response);
					node.close();
				}
				catch (Exception e)
				{
					node.close();
				}

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
				
				ChordData data;
				synchronized(dataMap)
				{
					data = dataMap.get(new String(hash));
				}

				if (data == null)
				{
					try
					{
						node.connect();
						node.sendMessage(ChordNode.MessageType.GET_REPLY_INVALID, null);
						node.close();
					}
					catch (Exception e)
					{
						node.close();
					}
				}
				else
				{
					response = ByteBuffer.allocate(data.getData().length);
					response.put(data.getData());
					
					try
					{
						node.connect();
						node.sendMessage(ChordNode.MessageType.GET_REPLY, response);
						node.close();
					}
					catch (Exception e)
					{
						node.close();
					}
				}
				
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

					if(existingData == null)
					{
						dataMap.put(new String(hash), new ChordData(hash, data));
					}
					else
					{
						byte[] newData = Arrays.copyOf(existingData.getData(), existingData.getData().length + data.length);
						System.arraycopy(data, 0, newData, existingData.getData().length, data.length);
						existingData.setData(newData);
					}
				}

				break;
			}
			case SUCCESSOR_LIST:
			{
				ByteBuffer response;
				
				synchronized(successorList)
				{
					response = ByteBuffer.allocate(6 * successorList.size());
					response.order(ByteOrder.BIG_ENDIAN);
				
					for (ChordNode c : successorList)
					{
						response.put(c.getIPAddress().getAddress());
						response.putShort((short)c.getPort());
					}
				}
				
				try
				{
					node.connect();
					node.sendMessage(ChordNode.MessageType.SUCCESSOR_LIST_REPLY, response);
					node.close();
				}
				catch (Exception e)
				{
					node.close();
				}

				break;
			}
			default:
			{
				throw new Exception("Unexpected message");
			}
		}
	}
}
