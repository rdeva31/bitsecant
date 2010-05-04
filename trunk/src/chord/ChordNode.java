package chord;

import java.io.*;
import java.net.*;
import java.nio.*;
import net.rudp.*;
import java.security.*;
import java.util.*;

public class ChordNode
{
	private byte[] hash;
	private ReliableSocket sock;
	private InetSocketAddress sockAddr;
	private BufferedInputStream sockIn;
	private BufferedOutputStream sockOut;

	public ChordNode(InetAddress IPAddr, short port) throws Exception
	{
		sock = null;
		sockAddr = new InetSocketAddress(IPAddr, port);
		sockIn = null;
		sockOut = null;
		
		byte[] identifier = Arrays.copyOf(IPAddr.getAddress(), 6);
		identifier[4] = (byte)((port >> 8) & 0xFF);
		identifier[5] = (byte)(port & 0xFF);
		hash = MessageDigest.getInstance("SHA").digest(identifier);
	}

	public ChordNode(ReliableSocket sock) throws Exception
	{
		this.sock = sock;
		sockAddr = (InetSocketAddress)sock.getRemoteSocketAddress();
		sockIn = new BufferedInputStream(sock.getInputStream());
		sockOut = new BufferedOutputStream(sock.getOutputStream());
		
		byte[] identifier = Arrays.copyOf(sockAddr.getAddress().getAddress(), 6);
		identifier[4] = (byte)((sockAddr.getPort() >> 8) & 0xFF);
		identifier[5] = (byte)(sockAddr.getPort() & 0xFF);
		hash = MessageDigest.getInstance("SHA").digest(identifier);
	}
	
	public ReliableSocket getSock()
	{
		return sock;
	}
	
	public InetSocketAddress getSockAddr()
	{
		return sockAddr;
	}
	
	public byte[] getHash()
	{
		return hash;
	}
	
	public InetAddress getIPAddress()
	{
		return sockAddr.getAddress();
	}
	
	public short getPort()
	{
		return (short)sockAddr.getPort();
	}
	
	public void connect() throws Exception
	{
		sock = new ReliableSocket(sockAddr.getAddress().getHostAddress(), sockAddr.getPort());
		sockIn = new BufferedInputStream(sock.getInputStream());
		sockOut = new BufferedOutputStream(sock.getOutputStream());
	}
	
	public void close() throws Exception
	{
		sock.close();
		sock = null;
	}
	
	public void sendMessage(MessageType type, ByteBuffer payload) throws Exception
	{
		if(sock == null)
		{
			connect();
		}

		byte[] message;

		if(payload == null)
		{
			message = new byte[5];

			message[0] = 0;
			message[1] = 0;
			message[2] = 0;
			message[3] = 1;

			message[4] = (byte)type.valueOf();
		}
		else
		{
			payload.flip();

			int messageLength = 1 + payload.remaining();
			message = new byte[4 + messageLength];

			message[0] = (byte)(messageLength >> 24);
			message[1] = (byte)(messageLength >> 16);
			message[2] = (byte)(messageLength >> 8);
			message[3] = (byte)(messageLength);

			message[4] = (byte)type.valueOf();

			payload.get(message, 5, messageLength - 1);
		}
		
		sockOut.write(message, 0, message.length);
	}
	
	public ByteBuffer getResponse() throws Exception
	{
		byte[] header = new byte[4];
		sockIn.read(header, 0, 4);
		
		int messageLength = (((int)header[0] & 0xFF) << 24) + (((int)header[1] & 0xFF) << 16) + 
							(((int)header[2] & 0xFF) << 8) + ((int)header[0] & 0xFF);
							
		byte[] message = new byte[messageLength];
		sockIn.read(message, 0, messageLength);
		
		return ByteBuffer.wrap(message);
	}
	
	private static int compare(byte[] hash1, byte[] hash2)
	{
		for (int c = hash1.length-1; c >= 0; --c)
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