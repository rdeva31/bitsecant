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
	private final int FINGER_TABLE_SIZE = HASH_SIZE * 8;
	private final int STABILIZE_TIMER_DELAY = 2*1000; 			//milliseconds
	private final int CHECK_PREDECESSOR_TIMER_DELAY = 2*1000; 	//milliseconds
	private final int FIX_FINGERS_TIMER_DELAY = 200; 			//milliseconds
	private final int SUCCESSOR_LIST_SIZE = 3;

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
		int port = 0;
		final int TIMER_PERIOD = 1000;
		
		//Checking the command line arguments
		for (start = 0; start < args.length; start++)
		{
			//Port to listen on for incoming messages
			if (args[start].equalsIgnoreCase("-listen") || args[start].equalsIgnoreCase("-l"))
			{
				chord = new Chord(Integer.parseInt(args[++start]));
			}
			//Node will join the ring by contacting the node at the specified port
			else if (args[start].equalsIgnoreCase("-join") || args[start].equalsIgnoreCase("-j"))
			{
				if (chord == null)
				{
					System.out.println("ERROR: Specify -listen before -join");
					return;
				}

				String address = args[++start];
				port = Integer.parseInt(args[++start]);

				chord.join(new ChordNode(InetAddress.getByName(address), (short)port));
			}
			//Node will create the ring
			else if (args[start].equalsIgnoreCase("-create") || args[start].equalsIgnoreCase("-c"))
			{
				if (chord == null)
				{
					System.out.println("ERROR: Specify -listen before -create");
					return;
				}
				
				chord.create();
			}
			//Unknown argument
			else
			{
				System.out.println("Usage: Chord -listen <port> [-create] [-join <all other ports>]");
				return;
			}
		}
		
		//Starting up the node
		chord.listen();
				
		//Reading commands from console input
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
			
			//Get <Key> - Gets the value that corresponds to the key
			if (cmd.equalsIgnoreCase("get"))
			{
				try
				{
					System.out.println("-> GOT: " + new String(chord.get(MessageDigest.getInstance("SHA-1").digest(arguments[0].getBytes())).getData()));
				}
				catch (Exception e)
				{
					System.out.println("-!> GOT: Failed... " + e.getMessage());
				}
			}
			//Put <Key> <Value> - Puts the key,value pair into the ring
			//Append <Key> <Value> - Appends the value onto the end of the value that corresponds to the key
			else if (cmd.equalsIgnoreCase("put") || cmd.equalsIgnoreCase("append"))
			{
				boolean append = cmd.equalsIgnoreCase("put") ? false : true;
				try
				{
					String data = arguments[1];
					
					for(int a = 2; a < arguments.length; a++)
					{
						data += " " + arguments[a];
					}
					
					chord.put(new ChordData(MessageDigest.getInstance("SHA-1").digest(arguments[0].getBytes()), data.getBytes()), append);
					System.out.println("-> PUT: Succeeded!");
				}
				catch (Exception e)
				{
					System.out.println("-!> PUT: Failed... " + e.getMessage());
				}
			}
			//Die - Kills the node
			else if (cmd.equalsIgnoreCase("die"))
			{
				System.out.println("-> Euthanizing node in an emergency intelligence incenerator."); 
				System.exit(0);
			}
			//Print - Prints information about the node
			else if (cmd.equalsIgnoreCase("print"))
			{
				System.out.println(chord);
			}
			//PrintFT - Prints the finger table
			else if (cmd.equalsIgnoreCase("printft"))
			{
				System.out.println(chord.fingerTableToString());
			}
			//Invalid Command
			else
			{
				System.out.println("-!> Invalid command");
			}
		}
	}

	/**
		Creates a node to interact with a chord ring
		@param port the port to listen to locally for incoming messages
	*/
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
		nextFingerToFix = 0;

		successorList = new LinkedList<ChordNode>();
		for (int s = 0; s < SUCCESSOR_LIST_SIZE; s++)
		{
			successorList.add(key);
		}

		dataMap = new HashMap<String, ChordData>();

		sock = new RUDPServerSocket(port);
	}

	/**
		Writes the finger table information to a string
		@return finger table string
	*/
	public String fingerTableToString()
	{
		String str = "";

		synchronized(fingerTable)
		{
			for(int i = 0; i < FINGER_TABLE_SIZE; i++)
			{
				str += "[" + i + "] " + fingerTable.get(i).toString() + "\n";
			}
		}

		return str;
	}
	
	/**
		Writes the information about the node to a string
		@return node information string
	*/
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

	/**
	 * Sets the successor of this node in all of the data structures. Also updates the
	 * new successor with any data that it needs to hold
	 * @param successor
	 */
	private void setSuccessor(ChordNode successor)
	{
		//Setting pu the new successor
		synchronized(successorList)
		{
			synchronized(fingerTable)
			{
				this.successor = successor;
				successorList.set(0, successor);
				fingerTable.set(0, successor);
			}
		}

		//Generating the data to send to the new successor
		LinkedList<ByteBuffer> sendList = new LinkedList<ByteBuffer>();

		synchronized(dataMap)
		{
			for(ChordData data : dataMap.values())
			{
				ByteBuffer toSend = ByteBuffer.allocate(HASH_SIZE + data.getData().length);
				toSend.put(data.getHash());
				toSend.put(data.getData());
				sendList.add(toSend);
			}
		}

		//Sending all of the necessary data to the new successor
		try
		{
			successor.connect();
			for(ByteBuffer toSend : sendList)
			{
				successor.sendMessage(ChordNode.MessageType.PUT, toSend);
			}
			successor.close();
		}
		catch (Exception e)
		{
			successor.close();
		}
	}

	/**
	 * Starts up the various timers that do periodic checks/updating as well as the
	 * main thread for receiving messages at port this node is listening on
	 * @throws Exception
	 */
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

		//Creating the thread to deal with receiving messages
		final RUDPServerSocket sockCopy = sock;
		new Thread(new Runnable()
		{
			public void run()
			{
				ByteBuffer readBuffer = ByteBuffer.allocateDirect(64*1024);
				
				while(true)
				{
					readBuffer.clear();
					
					try
					{
						//Reading the next message from the socket server
						final RUDPSocket client = sockCopy.read(readBuffer);
						final ByteBuffer message = ByteBuffer.allocate(readBuffer.remaining());
						
						message.order(ByteOrder.BIG_ENDIAN);
						message.put(readBuffer);
						message.flip();

						//Starting a new thread to handle message processing
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
									System.out.println("Exception: " + e);
									e.printStackTrace();
								}
							}
						}).start();
					}
					catch (Exception e)
					{
						System.out.println("Exception: " + e);
						e.printStackTrace();
						System.exit(-1);
					}
				}
			}
		}).start();
	}

	/**
	 * Creating the ring initially
	 * @throws Exception
	 */
	public void create() throws Exception
	{
		predecessor = null;
	}

	/**
	 * Attempting to join the ring by connecting to a node that is known to be on the ring
	 * @param node node on the ring
	 * @throws Exception Node could not be contacted
	 */
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

	/**
	 * Getting the list of data that is held on this node
	 * @return
	 */
	public Collection<ChordData> getLocalData()
	{
		synchronized(dataMap)
		{
			return new LinkedList(dataMap.values());
		}
	}

	/**
	 * Getting the data that corresponds to the hash from the ring
	 * @param hash hash key for the data
	 * @return the data that corresponds to the hash
	 * @throws Exception Data could not be found
	 */
	public ChordData get(byte[] hash) throws Exception
	{
		ChordNode node = findSuccessor(hash);
	
		ByteBuffer message = ByteBuffer.allocate(HASH_SIZE);
		message.put(hash);
		
		ByteBuffer response = null;

		//Connecting to the node who holds the data
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
			throw new Exception("Could not connect to node with data");
		}
		
		ChordNode.MessageType messageID = ChordNode.MessageType.fromInt((int)response.get() & 0xFF);

		//Node return the data
		if (messageID == ChordNode.MessageType.GET_REPLY)
		{
			byte[] toReturn = new byte[response.remaining()];
			response.get(toReturn);
			return new ChordData(hash, toReturn);
		}
		//Node did not have the data
		else if (messageID == ChordNode.MessageType.GET_REPLY_INVALID)
		{
			throw new Exception("Data does not exist");
		}
		//Unexpected message
		else
		{
			throw new Exception("Expected get_reply or get_reply_invalid");
		}
	}

	/**
	 * Putting the given data onto the ring, or appending it to data that
	 * already exists on the ring
	 * @param toPut data to put onto the ring
	 * @param append if the data should be appended to existing data
	 * @throws Exception Put operation failed
	 */
	public void put(ChordData toPut, boolean append) throws Exception
	{
		byte[] hash = toPut.getHash();
		byte[] data = toPut.getData();

		ByteBuffer toSend = ByteBuffer.allocate(HASH_SIZE + data.length);
		toSend.put(hash);
		toSend.put(data);
		
		ChordNode node = findSuccessor(hash);
		node.connect();
		node.sendMessage(append ? ChordNode.MessageType.APPEND : ChordNode.MessageType.PUT, toSend);
		node.close();
	}

	/**
	 * Removing a key,value pair corresponding to the hash from the chord node
	 * @param hash
	 */
	public void remove(byte[] hash)
	{
		synchronized(dataMap)
		{
			dataMap.remove(new String(hash));
		}
	}

	/**
	 * Finding the immediate successor to a given hash value
	 * @param hash hash value to find the successor of
	 * @return the immediate successor to the hash value
	 * @throws Exception Node could not be formed from the return IP,Port pair
	 */
	private ChordNode findSuccessor(byte[] hash) throws Exception
	{
		//Successor to the hash is our successor
		if (ChordNode.isInRange(hash, key.getHash(), false, successor.getHash(), true))
		{
			return successor;
		}
		//Searching for the closest node to the hash, then querying them
		else
		{
			ByteBuffer buffer = ByteBuffer.allocate(HASH_SIZE);
			buffer.put(hash);

			ChordNode closest = findClosestNode(hash);

			//Looping in order to find a the closest node that responds
			while(true)
			{
				//Case where we are actually the closest node
				if(closest.equals(key))
				{
					return successor;
				}

				//Asking the node for it's successor
				try
				{
					closest.connect();
					closest.sendMessage(ChordNode.MessageType.SUCCESSOR, buffer);
					buffer = closest.getResponse();
					closest.close();
					break;
				}
				//Finding the next closest node
				catch (Exception e)
				{
					closest.close();
					closest = findClosestNode(closest.getHash(), closest);
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

	/**
	 * Finds the position of the node in the fingertable which is closest to the hash value
	 * @param hash hash value
	 * @return closest node
	 */
	private ChordNode findClosestNode(byte[] hash)
	{
		synchronized(fingerTable)
		{
			for (int i = (FINGER_TABLE_SIZE - 1); i >= 0; i--)
			{
				ChordNode temp = fingerTable.get(i);
				if (ChordNode.isInRange(temp.getHash(), key.getHash(), false, hash, false))
				{
					return temp;
				}
			}
		}

		return key;
	}

	/**
	 * Finds the position of the node in the fingertable which is closest to the hash value
	 * @param hash hash value
	 * @param nodeToIgnore search happens below the first case of this node in the table
	 * @return closest node
	 */
	private ChordNode findClosestNode(byte[] hash, ChordNode nodeToIgnore)
	{
		synchronized(fingerTable)
		{
			int limit = fingerTable.indexOf(nodeToIgnore) - 1;

			if(limit == -1)
			{
				return key;
			}

			for (int i = limit; i >= 0; i--)
			{
				ChordNode temp = fingerTable.get(i);
				if (ChordNode.isInRange(temp.getHash(), key.getHash(), false, hash, false))
				{
					return temp;
				}
			}
		}

		return key;
	}

	/**
	 * Stabilizes the successor list by contacting the successor for it's list and
	 * using all but the last node to fill up our list
	 */
	private void stabilizeSuccessorList()
	{
		ByteBuffer response = null;

		//Getting our successor's successor list
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

		//Clearing and rebuilding the successor list based off the response
		synchronized(successorList)
		{	
			successorList.clear();
			successorList.add(successor);
			
			response.order(ByteOrder.BIG_ENDIAN);
			response.get(); //waste the message ID

			//Adding every node besides the last one
			while(response.remaining() > 6)
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

			//Too few successors to fill up to required size, so adding replicas of earlier entries
			int nextToReplicate = 0;
			while(successorList.size() < SUCCESSOR_LIST_SIZE)
			{
				successorList.add(successorList.get(nextToReplicate++));
			}
		}
	}

	/**
	 * Stabilizes the ring by checking our successor's predecessor and notifying them
	 * if they have the wrong predecessor currently. Also stablizies the successor list
	 * @throws Exception
	 */
	private void stabilize() throws Exception
	{
		ByteBuffer response;

		//Going through successors, and updating the successor list, until one responds
		//to a predecessor message
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

					//All of the successors failed, insert ourselves into the list
					if(successorList.size() == 0)
					{
						successorList.add(key);
					}

					setSuccessor(successorList.get(0));
				}
			}
		}

		//Stablizing the successor list
		stabilizeSuccessorList();
		
		byte[] IPAddress = new byte[4];
		short port;

		response.order(ByteOrder.BIG_ENDIAN);
		response.get(); //waste the message ID
		response.get(IPAddress);
		port = response.getShort();
		
		//Predecessor was null for our successor, so notify them of us
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

		//Successor's predecessor should actually be our successor
		if (ChordNode.isInRange(node.getHash(), key.getHash(), false, successor.getHash(), false))
		{
			setSuccessor(node);
			stabilizeSuccessorList();
		}
		//Notifying our successor that we should be its predecessor
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

	/**
	 * Notify from another node that it should be our new predecessor
	 * @param node new predecessor
	 * @throws Exception
	 */
	private void notify(ChordNode node) throws Exception
	{
		if ((predecessor == null) || ChordNode.isInRange(node.getHash(), predecessor.getHash(), false, key.getHash(), false))
		{
			predecessor = node;

			//Skipping propagation of data if we are our own predecessor
			if(predecessor.equals(key))
			{
				return;
			}

			//Generating the list of data to send
			LinkedList<ByteBuffer> sendList = new LinkedList<ByteBuffer>();
			
			synchronized(dataMap)
			{
				for(ChordData data : dataMap.values())
				{
					//Only sending data that our predecessor should be directly holding
					if(ChordNode.isInRange(data.getHash(), key.getHash(), false, predecessor.getHash(), false))
					{
						ByteBuffer toSend = ByteBuffer.allocate(HASH_SIZE + data.getData().length);
						toSend.put(data.getHash());
						toSend.put(data.getData());
						sendList.add(toSend);
					}
				}
			}

			//Sending the necessary data to our predecessor
			try
			{
				predecessor.connect();
				for(ByteBuffer toSend : sendList)
				{
					predecessor.sendMessage(ChordNode.MessageType.PUT, toSend);
				}
				predecessor.close();
			}
			catch (Exception e)
			{
				predecessor.close();
			}
		}
	}

	/**
	 * Fixes the next node in the finger table to hold the correct successor node
	 * @throws Exception Find successor failed to work
	 */
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

		synchronized(fingerTable)
		{
			if(!node.equals(fingerTable.get(nextFingerToFix)))
			{
				fingerTable.set(nextFingerToFix, node);
			}
		}
		
		nextFingerToFix = (nextFingerToFix + 1) % FINGER_TABLE_SIZE;
	}

	/**
	 * Checking our predecessor to see if they are still there, and adjusting if they
	 * failed
	 */
	private void checkPredecessor()
	{
		if(predecessor == null)
		{
			return;
		}
		
		ByteBuffer buffer = ByteBuffer.allocate(6);
		buffer.order(ByteOrder.BIG_ENDIAN);
		buffer.put(key.getIPAddress().getAddress());
		buffer.putShort(key.getPort());

		//Attempting to ping our predecessor
		try
		{
			predecessor.connect();
			predecessor.sendMessage(ChordNode.MessageType.PING, buffer);
			predecessor.getResponse();
			predecessor.close();
		}
		//Message was dropped, so our predecessor failed
		catch (Exception e)
		{
			predecessor.close();

			//Generating the list of data to send
			LinkedList<ByteBuffer> sendList = new LinkedList<ByteBuffer>();
			
			synchronized(dataMap)
			{
				for(ChordData data : dataMap.values())
				{
					//Only sending data that our predecessor should be directly holding
					if(ChordNode.isInRange(data.getHash(), key.getHash(), false, predecessor.getHash(), false))
					{
						ByteBuffer toSend = ByteBuffer.allocate(HASH_SIZE + data.getData().length);
						toSend.put(data.getHash());
						toSend.put(data.getData());
						sendList.add(toSend);
					}
				}
			}

			//Finding the last successor in our list
			ChordNode lastSuccessor;
			synchronized(successorList)
			{
				lastSuccessor = successorList.getLast();
			}

			//Skipping over sending the data if we are the last successor
			if(lastSuccessor.equals(key))
			{
				predecessor = null;
				return;
			}

			//Sending the data to the last successor in order to keep up the resiliency
			//of data in the ring
			try
			{
				lastSuccessor.connect();
				for(ByteBuffer toSend : sendList)
				{
					lastSuccessor.sendMessage(ChordNode.MessageType.PUT, toSend);
				}
				lastSuccessor.close();
			}
			catch (Exception ex)
			{
				lastSuccessor.close();
			}
		
			predecessor = null;
		}
	}

	/**
	 * Sending data to all of our successors in the list
	 * @param data data to send
	 */
	private void sendToSuccessors(ByteBuffer data)
	{
		//Getting the successors from the list
		LinkedList<ChordNode> successors;
		synchronized(successorList)
		{
			successors = new LinkedList<ChordNode>(successorList);
		}

		//Sending the data to the successor
		for(ChordNode node : successors)
		{
			//Skipping over sending the data if we are the current successor
			if(node.equals(key))
			{
				continue;
			}
		
			try
			{
				node.connect();
				node.sendMessage(ChordNode.MessageType.PUT, data);
				node.close();
			}
			catch (Exception e)
			{
				node.close();
			}
		}
	}

	/**
	 * Handling a message coming from a node on the ring
	 * @param buffer incoming message
	 * @param node sender of the message
	 * @throws Exception Mal-formed message
	 */
	private void handleMessage(ByteBuffer buffer, ChordNode node) throws Exception
	{
		//Handling the message based on the message type
		ChordNode.MessageType messageType = ChordNode.MessageType.fromInt((int)buffer.get() & 0xFF);
		switch (messageType)
		{
			//Ping -> Send a reply ping message back to let the node know we are alive
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
			//Successor -> Looking up the successor information for the given hash
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
			//Predecessor -> Replying back with our predecessor (or 0.0.0.0:0 to say we have
			//no predecessor currently)
			case PREDECESSOR:
			{
				ByteBuffer response = ByteBuffer.allocate(6);
				response.order(ByteOrder.BIG_ENDIAN);

				//IP and port of our predecessor
				if (predecessor != null)
				{
					response.put(predecessor.getIPAddress().getAddress());
					response.putShort((short)predecessor.getPort());
				}
				//No predecessor currently, so 0.0.0.0:0
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
			//Notify -> Node is letting us know it thinks it should be our predecessor
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
			//Get -> Node wants the data corresponding to a key held by us
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

				//Invalid get request, so letting the node know
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
				//Sending the corresponding data back to the node
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
			//Put -> Node wants us to hold a key,value data pair
			case PUT:
			{
				byte[] hash = new byte[HASH_SIZE];
				byte[] data = new byte[buffer.remaining() - HASH_SIZE];
				buffer.get(hash);
				buffer.get(data);

				//Updating the data in our list
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

				//Forwarding this data onto our successors if we are the direct holder
				if(predecessor == null || ChordNode.isInRange(hash, predecessor.getHash(), false, key.getHash(), false))
				{
					ByteBuffer toSend = ByteBuffer.allocate(HASH_SIZE + data.length);
					toSend.put(hash);
					toSend.put(data);
				
					sendToSuccessors(toSend);
				}
				
				break;
			}
			//Append -> Node wants to append data to a key,value pair. Or if the pair does
			//not exist, then create it with the given data
			case APPEND:
			{
				byte[] hash = new byte[HASH_SIZE];
				byte[] data = new byte[buffer.remaining() - HASH_SIZE];
				buffer.get(hash);
				buffer.get(data);
				
				ByteBuffer toSend;

				//Updating the data in our list
				synchronized(dataMap)
				{
					ChordData existingData = dataMap.get(new String(hash));

					//Creating new data
					if(existingData == null)
					{
						dataMap.put(new String(hash), new ChordData(hash, data));
				
						toSend = ByteBuffer.allocate(HASH_SIZE + data.length);
						toSend.put(hash);
						toSend.put(data);
					}
					//Appending to existing data
					else
					{
						existingData.appendData(data);
						
						byte[] updatedData = existingData.getData();
				
						toSend = ByteBuffer.allocate(HASH_SIZE + updatedData.length);
						toSend.put(hash);
						toSend.put(updatedData);
					}
				}

				//Forwarding this data onto our successors if we are the direct holder
				if(predecessor == null || ChordNode.isInRange(hash, predecessor.getHash(), false, key.getHash(), false))
				{
					sendToSuccessors(toSend);
				}

				break;
			}
			//Successor List -> Node wants our successor list
			case SUCCESSOR_LIST:
			{
				ByteBuffer response;

				//Generating the response containg our successors' information
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
			//Removing a given key,value data pair from our data list
			case REMOVE:
			{
				byte[] hash = new byte[HASH_SIZE];
				buffer.get(hash);
				
				dataMap.remove(new String(hash));
				
				break;
			}
			//Unexpected message
			default:
			{
				throw new Exception("Unexpected message");
			}
		}
	}
}
