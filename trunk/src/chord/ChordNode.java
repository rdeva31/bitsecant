package chord;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.security.*;

public class ChordNode
{
	
	private byte[] hash;
	private DatagramSocket sock;
	private InetSocketAddress sockAddr;

	public ChordNode(byte[] hash, InetAddress IPAddr, int port) throws Exception
	{
		sockAddr = new InetSocketAddress(IPAddr, port);
	}
	
	public DatagramChannel getSock()
	{
		return sock;
	}
	
	public SocketAddress getSockAddr()
	{
		return sockAddr;
	}
	
	public enum MessageType {
		PING(0),
		SUCCESSOR(1),
		PREDECESSOR(2),
		NOTIFY(3),
		GET(4),
		PUT(5);
		
		private int value;
		MessageType(int value)
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