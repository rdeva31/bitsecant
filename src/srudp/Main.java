package srudp;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.security.*;
import java.util.*;

public class Main
{
    public static void main(String[] args) throws Exception
	{

		new Thread(new Runnable()
		{
			public void run()
			{
				try
				{
					RUDPServerSocket server = new RUDPServerSocket(1337);
					ByteBuffer readBuffer = ByteBuffer.allocate(64*1024);

					while(true)
					{
						readBuffer.clear();
						RUDPSocket sender = server.read(readBuffer);
						byte[] read = new byte[readBuffer.remaining()];
						readBuffer.get(read);

						System.out.println("RECEIVED: " + new String(read));
					}
				}
				catch (Exception e)
				{
					System.out.println("SERVER");
					e.printStackTrace();
				}
			}
		}).start();

		RUDPSocket client = new RUDPSocket(InetAddress.getLocalHost(), 1337);
		byte[] sendBuffer = ("Hello World!").getBytes();
		client.write(sendBuffer);
		client.write(sendBuffer);
		client.write(sendBuffer);
    }
}
