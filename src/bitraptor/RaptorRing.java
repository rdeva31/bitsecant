package bitraptor;

import java.util.*;
import chord.*;

public class RaptorRing
{
	private int port;
	private Chord chord;

	private final int PEER_EXPIRER_TIMER_PERIOD = 2*1000; //in milliseconds
	
	public RaptorRing(int port) throws Exception
	{
		this.port = port;
		
		//Create the chord ring
		try
		{
			chord = new Chord(port);
			chord.create();
			chord.listen();
		}
		catch (Exception e)
		{
			System.out.println("ERROR: Failed to create chord ring");
			System.exit(-1);
		}
	}
	
	public void start()
	{
		//Starting up the peer expirer timer
		Timer peerExpirerTimer = new Timer(false);
		peerExpirerTimer.scheduleAtFixedRate(new ExpirePeersTimer(), PEER_EXPIRER_TIMER_PERIOD, PEER_EXPIRER_TIMER_PERIOD);

		//Do nothing
		while(true)
		{
			try
			{
				Thread.sleep(1000*1000);
			}
			catch (Exception e)
			{
			}
		}
	}

	private class ExpirePeersTimer extends TimerTask
	{
		public void run()
		{
			Collection<ChordData> localData = chord.getLocalData();

			//Nothing to process in our local data
			if(localData.size() == 0)
			{
				return;
			}

			//Going through all of the local data
			for (ChordData data : localData)
			{
				//Initializing to parse the data
				RaptorData rawr;
				try
				{
					rawr = new RaptorData(data.getHash(), data.getData());
				}
				catch (Exception e)
				{
					continue;
				}

				//Aging the peers, and expiring any that reached TTL = 0
				rawr.agePeers();
				rawr.expirePeers();

				//Generating the revised data and storing it, or if null, removing it
				byte[] revisedData = rawr.getData();

				if(revisedData == null)
				{
					chord.remove(data.getHash());
				}
				else
				{
					data.setData(revisedData);
				}
			}
		}
	}
}
