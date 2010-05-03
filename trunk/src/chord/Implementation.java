package chord;

public class Implementation
{
	public static void main(String [] args) throws Exception
	{
		if (args == null || args.length == 0)
			System.out.println("usage: <program> <num_clients_to_start> <port_of_first_client>");
		
		int numClients = Integer.parseInt(args[0]);
		int port = Integer.parseInt(args[1]);
		Chord first = null;
		for (int c = 0; c < numClients; ++c)
		{
			final Chord chord = new Chord(port++);
			
			if (first == null)
			{
				first = chord;
			}
			
			final Chord firstChord = first;
			
			new Thread(new Runnable()
			{
				public void run()
				{
					try
					{
						chord.join(firstChord.getKey());
						chord.listen();
					}
					catch (Exception e)
					{
						System.out.println("Exception: " + e);
						e.printStackTrace();
					}
				}
			}).run();
		}
	}
}