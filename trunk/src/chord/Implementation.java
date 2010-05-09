package chord;

import java.net.*;
import java.security.MessageDigest;
import java.util.*;

public class Implementation
{
	public static void main(String [] args) throws Exception
	{
		List<Thread> threadList = new ArrayList<Thread>();
		if (args == null || args.length == 0)
		{
			System.out.println("usage: <program> <num_clients_to_start> <port_of_first_client>");
			return;
		}

		final int numClients = Integer.parseInt(args[0]);
		final int port = Integer.parseInt(args[1]);
		
		final Chord first = new Chord(port);
		first.create();

		for (int c = 0; c < numClients; c++)
		{
			final int curPort = port + c;
			final int cCopy = c;
			
			if(c == 0)
			{
				new Thread(new Runnable()
				{
					public void run()
					{
						try
						{
							first.listen();
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
				Thread t = new Thread(new Runnable()
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
				}, "thread" + c);

				t.start();
				
				threadList.add(t);
			}
		}
		
		final List<Thread> threadListCopy = threadList;
		int threadToKill;
		Scanner sc = new Scanner(System.in);
		
		System.out.print("kill which thread?");
		threadToKill = sc.nextInt();
		final int threadToKillCopy = threadToKill;
		new Timer().schedule(new TimerTask()
		{
			public void run()
			{
				try
				{
				
					first.put(MessageDigest.getInstance("SHA-1").digest("mudkipz".getBytes()), "I LIKE MUDKIPZ".getBytes(), false);
					threadListCopy.remove(threadToKillCopy).stop();
					new Timer().schedule(new TimerTask() 
					{
						public void run() {} //nothing to do
					}, 5*1000);//give it time to fix ring
					System.out.println(new String(first.get(MessageDigest.getInstance("SHA-1").digest("mudkipz".getBytes()))));
					
					System.out.println(first.toString());
					for(int i = 0; i < threadListCopy.size(); i++)
					{
						System.out.println(threadListCopy.get(i).toString());
					}
				}
				catch (Exception e)
				{
					System.out.println("Exception [MAIN] : " + e);
					e.printStackTrace();
				}
			}
		}, 1000 * 10);
		
		while(true);
	}
	
}