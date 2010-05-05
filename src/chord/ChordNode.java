package chord;

import srudp.*;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.security.*;
import java.util.*;

public class ChordNode
{
	private byte[] hash;
	private RUDPSocket sock;
	private InetAddress IPAddr;
	private short port;

	public ChordNode(InetAddress IPAddr, short port) throws Exception
	{
		sock = null;
		this.IPAddr = IPAddr;
		this.port = port;
		
		byte[] identifier = Arrays.copyOf(IPAddr.getAddress(), 6);
		identifier[4] = (byte)((port >> 8) & 0xFF);
		identifier[5] = (byte)(port & 0xFF);
		hash = MessageDigest.getInstance("SHA").digest(identifier);
	}

	public ChordNode(RUDPSocket sock) throws Exception
	{
		this.sock = sock;
		this.IPAddr = sock.getSockAddr().getAddress();
		this.port = (short)sock.getSockAddr().getPort();
		
		byte[] identifier = Arrays.copyOf(sock.getSockAddr().getAddress().getAddress(), 6);
		identifier[4] = (byte)((sock.getSockAddr().getPort() >> 8) & 0xFF);
		identifier[5] = (byte)(sock.getSockAddr().getPort() & 0xFF);
		hash = MessageDigest.getInstance("SHA").digest(identifier);
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
	
	public void connect() throws Exception
	{
		if(sock == null)
		{
			sock = new RUDPSocket(IPAddr, port);
		}
	}
	
	public void close() throws Exception
	{
		if(sock != null)
		{
			sock.close();
			sock = null;
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
	
	private static int compare(byte[] hash1, byte[] hash2)
	{
		for (int c = 0; c < hash1.length; ++c)
		{
			if (hash1[c] > hash2[c])
				return 1;
			else if (hash1[c] < hash2[c])
				return -1;
		}
		
		return 0;
	} 
	
	public static boolean isInRange(byte[] variant,
		byte[] lowerBound, boolean lowerBoundInclusive, 
		byte[] upperBound, boolean upperBoundInclusive)
	{
		if (ChordNode.compare(upperBound, lowerBound) < 0)
		{
			return (ChordNode.compare(upperBound, variant) < 0 || (upperBoundInclusive ? ChordNode.compare(upperBound, variant) == 0 : false)) 
			&& (ChordNode.compare(lowerBound, variant) > 0 || (lowerBoundInclusive ? ChordNode.compare(lowerBound, variant) == 0 : false));
		}
		else if (ChordNode.compare(upperBound, lowerBound) == 0)
		{
			return ChordNode.compare(variant, lowerBound) == 0;
		}
		else
		{
			return (ChordNode.compare(lowerBound, variant) < 0 || (lowerBoundInclusive ? ChordNode.compare(lowerBound, variant) == 0 : false)) 
			&& (ChordNode.compare(upperBound, variant) > 0 || (upperBoundInclusive ? ChordNode.compare(upperBound, variant) == 0 : false)); 
		}
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
		ADD(10);
		
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