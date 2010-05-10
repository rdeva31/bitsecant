package chord;

import srudp.*;
import java.net.*;
import java.nio.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.locks.*;

public class ChordNode
{
	private byte[] hash;
	private RUDPSocket sock;
	private InetAddress IPAddr;
	private short port;
	private Lock socketLock;

	/**
	 * Instantiates a new ChordNode based on the ip address and the port.
	 * @param IPAddr ip address of node you wish to connect to
	 * @param port port of the node listening for connections
	 * @throws Exception thrown if IPAddress is invalid in anyway
	 */
	public ChordNode(InetAddress IPAddr, short port) throws Exception
	{
		sock = null;
		this.IPAddr = IPAddr;
		this.port = port;

		byte[] identifier = Arrays.copyOf(IPAddr.getAddress(), 6);
		identifier[4] = (byte)((port >> 8) & 0xFF);
		identifier[5] = (byte)(port & 0xFF);
		hash = MessageDigest.getInstance("SHA-1").digest(identifier);

		socketLock = new ReentrantLock();
	}

	/**
	 * Associates this code with the socket
	 * @param sock socket used for talking with other Chords
	 * @throws Exception if sock is invalid in anyway or contains an invalid address/port
	 */
	public ChordNode(RUDPSocket sock) throws Exception
	{
		this.sock = sock;
		this.IPAddr = sock.getSockAddr().getAddress();
		this.port = (short)sock.getSockAddr().getPort();

		byte[] identifier = Arrays.copyOf(sock.getSockAddr().getAddress().getAddress(), 6);
		identifier[4] = (byte)((sock.getSockAddr().getPort() >> 8) & 0xFF);
		identifier[5] = (byte)(sock.getSockAddr().getPort() & 0xFF);
		hash = MessageDigest.getInstance("SHA-1").digest(identifier);

		socketLock = new ReentrantLock();
	}

	/**
	 * Returns the socket used in talking to the node
	 * @return socket used or null, if none used
	 */
	public RUDPSocket getSock()
	{
		return sock;
	}

	/**
	 * IP address of other socket
	 * @return ip address--never null
	 */
	public InetAddress getIPAddress()
	{
		return IPAddr;
	}

	/**
	 * Returns the port used in talking to the other socket
	 * @return the port
	 */
	public short getPort()
	{
		return port;
	}

	/**
	 * SHA-1 hash of the IP address and port
	 * @return SHA-1 hash--never null
	 */
	public byte[] getHash()
	{
		return hash;
	}

	/**
	 * A human readable string of getHash()
	 * @return string of getHash()
	 */
	public String getHashString()
	{
		String hashString = "";

		for(int b = 0; b < hash.length; b++)
		{
			int value = (int)hash[b] & 0xFF;

			if(value < 0x10)
			{
				hashString += "0";
			}

			hashString += Integer.toHexString(value);
		}

		return hashString.toUpperCase();
	}

	/**
	 * Connects to other node
	 * @throws Exception on network I/O error
	 */
	public void connect() throws Exception
	{
		try
		{
			socketLock.lock();
			if(sock == null)
			{
				sock = new RUDPSocket(IPAddr, port);
			}
		}
		catch (Exception e)
		{
			socketLock.unlock();
			throw e;
		}
	}

	/**
	 * Closes the socket used to talk with other node
	 */
	public void close()
	{
		try
		{
			if(sock != null)
			{
				sock.close();
				sock = null;
				socketLock.unlock();
			}
		}
		catch (Exception e)
		{
			sock = null;
			socketLock.unlock();
		}
	}

	/**
	 * Sends message to other node
	 * @param type type of message
	 * @param payload message contents
	 * @throws Exception thrown on Network I/O errors
	 */
	public void sendMessage(MessageType type, ByteBuffer payload) throws Exception
	{
		byte[] message;

		if(payload == null)
		{
			message = new byte[1];
			message[0] = (byte)type.valueOf();
		}
		else
		{
			payload.flip();

			int messageLength = 1 + payload.remaining();

			message = new byte[messageLength];
			message[0] = (byte)type.valueOf();
			payload.get(message, 1, messageLength - 1);
			
			payload.position(0);
			payload.compact();
		}

		sock.write(message);
	}

	/**
	 * Returns the response from other node.  Only usable after calling
	 * sendMessage().  Some MessageTypes don't require a response. Using this
	 * after those kinds of sendMessage() will result in an exception.
	 * @return reponse by other other
	 * @throws Exception in Network I/O error or if other node isn't interested
	 * in responding
	 */
	public ByteBuffer getResponse() throws Exception
	{
		return sock.read();
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
		final ChordNode other = (ChordNode) obj;
		if (!Arrays.equals(other.getHash(), hash))
		{
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 31 * hash + Arrays.hashCode(this.hash);
		return hash;
	}

	/**
	 * Compares two hashes.  Same specs as Comparator.compare()
	 * @param hash1 first hash to compare
	 * @param hash2 second hash to compare
	 * @return positive number of hash1 > hash2, zero if hash1 == hash2, negative
	 * otherwise
	 */
	public static int compare(byte[] hash1, byte[] hash2)
	{
		for (int c = 0; c < hash1.length; c++)
		{
			int a = ((int)hash1[c]) & 0xff;
			int b = ((int)hash2[c]) & 0xff;

			if (a > b)
			{
				return 1;
			}
			else if (a < b)
			{
				return -1;
			}
		}

		return 0;
	}

	/**
	 * Determines if this condition holds: lowerBound &lt; variant &gt; upperBound.
	 * @param variant point to compare with upper and lower bounds
	 * @param lowerBound lower bound
	 * @param lowerBoundInclusive if lower bound is inclusive
	 * @param upperBound upper bound
	 * @param upperBoundInclusive if upperbound is inclusive
	 * @return true if variant is in range, false otherwise
	 */
	public static boolean isInRange(byte[] variant,
		byte[] lowerBound, boolean lowerBoundInclusive,
		byte[] upperBound, boolean upperBoundInclusive)
	{
		int UL = ChordNode.compare(upperBound, lowerBound);
		int UV = ChordNode.compare(upperBound, variant);
		int LV = ChordNode.compare(lowerBound, variant);

		boolean UgV = upperBoundInclusive ? UV >= 0 : UV > 0;
		boolean UlV = upperBoundInclusive ? UV <= 0 : UV < 0;
		boolean LgV = lowerBoundInclusive ? LV >= 0 : LV > 0;
		boolean LlV = lowerBoundInclusive ? LV <= 0 : LV < 0;

		if(UL < 0)
		{
			return (UgV && LgV) || (UlV && LlV);
		}
		else if(UL == 0)
		{
			return (upperBoundInclusive && lowerBoundInclusive) ? true : (UV != 0);
		}
		else
		{
			return UgV && LlV;
		}
	}

	public String toString()
	{
		return "[" + port + "] " + getHashString();
	}

	/**
	 * Specifies the types of messages sendMessage() can send and getResponse()
	 * will return
	 */
	public enum MessageType {
		PING(0),
		PING_REPLY(1),
		SUCCESSOR(2),
		SUCCESSOR_REPLY(3),
		PREDECESSOR(4),
		PREDECESSOR_REPLY(5),
		NOTIFY(6),
		GET(7),
		GET_REPLY(8),
		PUT(9),
		APPEND(10),
		GET_REPLY_INVALID(11),
		SUCCESSOR_LIST(12),
		SUCCESSOR_LIST_REPLY(13),
		REMOVE(14);

		private int value;
		private MessageType(int value)
		{
			this.value = value;
		}

		public int valueOf()
		{
			return value;
		}

		/**
		 * Returns the Enum form fo the value val.
		 * @param val value to convert to enum
		 * @return corresponding Enum
		 * @throws Exception if value is not representable as Enum
		 */
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
}