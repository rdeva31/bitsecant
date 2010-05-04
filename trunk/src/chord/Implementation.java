package chord;

import java.net.*;

public class Implementation
{
	public static void main(String [] args) throws Exception
	{
		if (args == null || args.length == 0)
		{
			System.out.println("usage: <program> <num_clients_to_start> <port_of_first_client>");
			return;
		}

		int numClients = Integer.parseInt(args[0]);
		final int port = Integer.parseInt(args[1]);

		for (int c = 0; c < numClients; c++)
		{
			final int curPort = port + c;

			if (c == 0)
			{
				new Thread(new Runnable()
				{
					public void run()
					{
						try
						{
							Chord chord = new Chord(curPort);
							chord.create();
							chord.listen();
						}
						catch (Exception e)
						{
							System.out.println("Exception [" + curPort + "] : " + e);
							e.printStackTrace();
						}
					}
				}, "thread" + c).start();
			}
			else
			{
				new Thread(new Runnable()
				{
					public void run()
					{
						try
						{
							Chord chord = new Chord(curPort);
							chord.join(new ChordNode(InetAddress.getLocalHost(), (short)port));
							chord.listen();
						}
						catch (Exception e)
						{
							System.out.println("Exception [" + curPort + "] : " + e);
							e.printStackTrace();
						}
					}
				}, "thread" + c).start();
			}
		}
	}
}