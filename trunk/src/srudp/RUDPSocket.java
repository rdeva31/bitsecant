package srudp;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.security.*;
import java.util.*;

public class RUDPSocket
{
	private DatagramChannel sock;
	private InetSocketAddress sockAddr;
	private Selector select;

	public RUDPSocket(InetAddress IPAddr, int port) throws Exception
	{
		sock = DatagramChannel.open();
		sock.configureBlocking(false);
		sockAddr = new InetSocketAddress(IPAddr, port);
	}
	
	public InetSocketAddress getSockAddr()
	{
		return sockAddr;
	}
	
	public void close() throws Exception
	{
		sock.close();
	}

	public void write(byte[] payload) throws Exception
	{
		byte[] hash = MessageDigest.getInstance("SHA-1").digest(payload);
		ByteBuffer packet = ByteBuffer.allocate(payload.length + hash.length);
		packet.put(payload);
		packet.put(hash);
		packet.flip();

		Selector select = null;
		
		while(select == null)
		{
			try
			{
				select = Selector.open();
			}
			catch (Exception e)
			{
			}
		}
		
		SelectionKey key = sock.register(select, SelectionKey.OP_READ);

		int retryCount = 0;
		while(retryCount < 3)
		{
			packet.position(0);
			while(sock.send(packet, sockAddr) == 0)
			{
			}
			
			select.select(100);
			
			Iterator it = select.selectedKeys().iterator();
			if(it.hasNext())
			{
				SelectionKey selected = (SelectionKey)it.next();
				it.remove();

				if (selected.isReadable())
				{
					DatagramChannel curSock = (DatagramChannel)selected.channel();

					ByteBuffer response = ByteBuffer.allocate(20);
					curSock.receive(response);
					response.flip();

					if(response.equals(ByteBuffer.wrap(hash)))
					{
						selected.cancel();
						select.close();
						return;
					}
				}
			}

			retryCount++;
		}
		
		key.cancel();
		select.close();
	}

	public ByteBuffer read() throws Exception
	{
		ByteBuffer packet = ByteBuffer.allocate(64*1024);

		while(true)
		{
			packet.clear();
			while(sock.receive(packet) == null)
			{
			}
			packet.flip();

			if(packet.remaining() < 20)
			{
				continue;
			}

			byte[] payload = new byte[packet.remaining() - 20];
			packet.get(payload);
			byte[] hash = new byte[20];
			packet.get(hash);
			byte[] calcHash = MessageDigest.getInstance("SHA-1").digest(payload);

			if(Arrays.equals(hash, calcHash))
			{
				ByteBuffer response = ByteBuffer.wrap(hash);
				sock.send(response, sockAddr);
				
				return ByteBuffer.wrap(payload);
			}
		}
	}
}
