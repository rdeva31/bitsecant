package chord;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.security.*;

public class ChordNode
{
	
	private byte[] hash;
	private SocketChannel sock;
	private InetSocketAddress sockAddr;

	public ChordNode(InetAddress IPAddr, short port) throws Exception
	{
		sockAddr = new InetSocketAddress(IPAddr, port);
		
		byte[] identifier = Arrays.copyOf(IPAddr.getAddress(), 6);
		identifier[4] = (byte)((port >> 8) & 0xFF);
		identifier[5] = (byte)(port & 0xFF);
		hash = MessageDigest.getInstance("SHA").digest(identifier);
	}
	
	public SocketChannel getSock()
	{
		return sock;
	}
	
	public SocketAddress getSockAddr()
	{
		return sockAddr;
	}
	
	public ByteBuffer getReadBuffer()
	{
		return readBuffer;
	}
	
	public ByteBuffer getWriteBuffer()
	{
		return writeBuffer;
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
		return sockAddr.getPort();
	}
	
	public void connect() throws Exception
	{
		sock = SocketChannel.open();
		sock.configureBlocking(true);
		sock.socket().setReuseAddress(true);
		sock.connect();
		
		while(!sock.finishConnect())
		{
		}
	}
	
	public void writeMessage(MessageType type, ByteBuffer payload) throws Exception
	{
		ByteBuffer message = ByteBuffer.allocate(5 + payload.remaining());
		message.order(ByteOrder.BIG_ENDIAN);
		
		message.putInt(1 + payload.remaining());
		message.put((byte)type.valueOf());
		payload.mark();
		message.put(payload);
		payload.reset();
		
		message.flip();
		sock.write(message);
	}
	
	public ByteBuffer getResponse() throws Exception
	{
		ByteBuffer header = ByteBuffer.allocate(4);
		header.order(ByteOrder.BIG_ENDIAN);
		
		sock.read(header);
		header.flip();
		
		int messageLength = header.getInt();
		
		ByteBuffer message = ByteBuffer.allocate(messageLength);
		message.order(ByteOrder.BIG_ENDIAN);
		
		sock.read(message);
		message.flip();
		
		return message;
	}
	
	
	public boolean isBefore(ChordNode obj)
	{
		return isBefore(obj.hash);
	}
	
	public boolean isBefore(byte[] hash)
	{
		if (hash.length != this.hash.length)
			throw new Exception("mismatched hash length");
		return Arrays.compareTo(this.hash, hash) < 0;
	}
	
	public boolean isAfter(byte[] hash)
	{
		return !isBefore(hash);
	}
	
	public boolean isAfter(ChordNode obj)
	{
		return isAfter(obj.hash);
	}
	
	public boolean isSame(Chord obj)
	{
		return isSame(obj.hash);
	}
	
	public boolean isSame(byte[] hash)
	{
		return Arrays.equals(hash, this.hash);
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