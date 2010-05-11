package bitraptor;

import chord.ChordData;
import java.net.InetAddress;
import java.util.*;


public class RaptorData extends ChordData 
{
	public class PeerData
	{
		InetAddress IPAddr; 
		short port, ttl;
		
		public PeerData()
		{
		}
		
		public PeerData(InetAddress IPAddr, short port)
		{
			this.IPAddr = IPAddr;
			this.port = port;
		}
		
		public int getPort()
		{
			return port;
		}
		
		public int getTTL()
		{
			return ttl;
		}
		
		public InetAddress getIPAddress()
		{
			return IPAddr;
		}
		@Override
		public boolean equals(Object obj)
		{
			if (obj == null)
			{
				return false;
			}
			if (getClass() != obj.getClass())
			{
				return false;
			}
			final PeerData other = (PeerData) obj;
			if (!this.IPAddr.equals(other.IPAddr))
			{
				return false;
			}
			if (this.port != other.port)
			{
				return false;
			}
			return true;
		}

		@Override
		public int hashCode()
		{
			int hash = 3;
			hash = 97 * hash + (this.IPAddr != null ? this.IPAddr.hashCode() : 0);
			return hash;
		}
	} 
	
	private List<PeerData> peerList;
	private final int CONTAINER_SIZE = (4 + Short.SIZE/8 + Short.SIZE/8);
	private final int MAX_TTL = 50;
	
	public RaptorData (byte[] hash, byte[] data) throws Exception
	{
		super(hash, data);

		if (data.length % CONTAINER_SIZE != 0)
		{
			throw new Exception("data.length must be multiple of " + CONTAINER_SIZE);
		}

		peerList = parseArray(data);
	}
	
	@Override
	public byte[] getData()
	{
		byte[] data = new byte[peerList.size() * CONTAINER_SIZE];
		int dataIndex = 0;
		for (int c = 0; c < peerList.size(); ++c)
		{
			PeerData container = peerList.get(c);
			byte[] IPAddr = container.IPAddr.getAddress();
			data[dataIndex++] = IPAddr[0];
			data[dataIndex++] = IPAddr[1];
			data[dataIndex++] = IPAddr[2];
			data[dataIndex++] = IPAddr[3];

			short port = container.port;
			data[dataIndex++] = (byte)((port>>8) & 0xff);
			data[dataIndex++] = (byte)(port & 0xff);

			short ttl = container.ttl;
			data[dataIndex++] = (byte)((ttl>>8) & 0xff);
			data[dataIndex++] = (byte)(ttl & 0xff);
		}
		return data;
	}

	@Override
	public void setData(byte[] newData)
	{
		try
		{
			peerList = parseArray(newData);
		}
		catch (Exception e)
		{
			//do nothing
		}
	}
	
	@Override
	public void appendData(byte[] data)
	{
		try
		{
			List<PeerData> peerList = parseArray(data);
			for (PeerData c : peerList)
			{
				int index = this.peerList.indexOf(c);
				if (index < 0) //doesn't have, so add
				{
					this.peerList.add(c);
				}
				else
				{
					this.peerList.get(index).ttl = MAX_TTL;
				}
			}
		}
		catch (Exception e)
		{
			//do nothing
		}
	}
	
	public List<PeerData> getPeerList()
	{
		return peerList;
	}
	
	public void expirePeers()
	{
		Set <PeerData> toRemove  = new HashSet<PeerData>();
		Set <PeerData> serviced = new HashSet<PeerData>();
		
		Collections.sort(peerList, new Comparator<PeerData>()
		{
			public int compare(PeerData a, PeerData b)
			{
				return -1*(a.ttl - b.ttl);
			}
		});
		
		for (PeerData c : peerList)
		{
			if (c.ttl <= 0 || serviced.contains(c))
			{
				toRemove.add(c);
			}
			serviced.add(c);
		}
		peerList.removeAll(toRemove);
	}
	
	public void agePeers()
	{
		for (PeerData c : peerList)
		{
			c.ttl--;
		}
	}

	private List<PeerData> parseArray(byte[] data) throws Exception
	{
		if (data.length % CONTAINER_SIZE != 0)
			throw new Exception("data.length must be multiple of " + CONTAINER_SIZE);

		List<PeerData> l = new ArrayList<PeerData>(data.length);
		for (int c = 0; c < data.length;)
		{
			PeerData container = new PeerData();
			
			byte[] IPAddr = new byte[4];
			IPAddr[0] = data[c++];
			IPAddr[1] = data[c++];
			IPAddr[2] = data[c++];
			IPAddr[3] = data[c++];
			
			container.IPAddr = InetAddress.getByAddress(IPAddr);
			container.port = (short)((((int)data[c++] & 0xFF) << 8) | ((int)data[c++] & 0xFF));
			container.ttl = (short)((((int)data[c++] & 0xFF) << 8) | ((int)data[c++] & 0xFF));
			l.add(container);
		}

		return l;
	}
}
