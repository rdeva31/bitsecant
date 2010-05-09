package chord;

import srudp.*;
import java.io.*;
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

	public RUDPSocket getSock()
	{
		return sock;
	}

	public InetAddress getIPAddress()
	{
		return IPAddr;
	}

	public short getPort()
	{
		return port;
	}

	public byte[] getHash()
	{
		return hash;
	}

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

	public void close()
	{
		try
		{
			if(sock != null)
			{
				sock.close();
				sock = null;
			}
		}
		catch (Exception e)
		{
			sock = null;
		}
		finally
		{
			socketLock.unlock();
		}
	}

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
		SUCCESSOR_LIST_REPLY(13);

		private int value;
		private MessageType(int value)
		{
			this.value = value;
		}

		public int valueOf()
		{
			return value;
		}

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