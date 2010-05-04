package srudp;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.security.*;
import java.util.*;

public class RUDPServerSocket
{
	private DatagramChannel sock;

	public RUDPServerSocket(int port) throws Exception
	{
		sock = DatagramChannel.open();
		sock.socket().bind(new InetSocketAddress(port));
	}

	public RUDPSocket read(ByteBuffer buffer) throws Exception
	{
		ByteBuffer packet = ByteBuffer.allocate(64*1024);

		while(true)
		{
			packet.clear();
			InetSocketAddress returnAddr = (InetSocketAddress)sock.receive(packet);
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
				int rand = (int)(Math.random() * 2.0) % 2;

				ByteBuffer response = ByteBuffer.wrap(hash);
				sock.send(response, returnAddr);

				buffer.put(payload);
				buffer.flip();

				return new RUDPSocket(returnAddr.getAddress(), returnAddr.getPort());
			}
		}
	}
}
