package bitraptor;

import java.util.*;
import chord.*;

public class RaptorRing
{
	private int port;
	private Chord chord;

	private final int PEER_EXPIRER_TIMER_PERIOD = 60*1000; //in milliseconds
	
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
		
		while(true)
		{
			try
			{
				Thread.sleep(1000*1000);
			}
			catch (Exception e)
			{
				//Do nothing
			}
		}
	}

	private class ExpirePeersTimer extends TimerTask
	{
		public void run()
		{
			Collection<ChordData> coll = chord.getLocalData();
			if(coll.size() == 0)
			{
				return;
			}

			for (ChordData c : coll)
			{
				RaptorData rawr = null;
				try
				{
					rawr = new RaptorData(c.getHash(), c.getData());
				}
				catch (Exception e)
				{
					System.out.println("length was not multiple of 8");
					return;
				}
				
				rawr.agePeers();
				rawr.expirePeers();
				c.setData(rawr.getData());
			}

			chord.removeGarbage();
		}
	}
	
}
