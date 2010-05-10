package bitsecant;

import chord.ChordData;
import java.net.InetAddress;
import java.util.*;


public class RaptorData extends ChordData 
{
	private class Container 
	{
		InetAddress IPAddr; 
		short port, ttl;

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
			final Container other = (Container) obj;
			if (this.IPAddr != other.IPAddr && (this.IPAddr == null || !this.IPAddr.equals(other.IPAddr)))
			{
				return false;
			}
			if (this.port != other.port)
			{
				return false;
			}
			if (this.ttl != other.ttl)
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
	
	private List<Container> peerList;
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
			Container container = peerList.get(c);
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
			List<Container> peerList = parseArray(data);
			for (Container c : peerList)
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
	
	public void expirePeers()
	{
		Set <Container> toRemove  = new HashSet<Container>();
		Set <String> serviced = new HashSet<String>();
		
		Collections.sort(peerList, new Comparator<Container>()
		{
			public int compare(Container a, Container b)
			{
				return -1*(a.ttl - b.ttl);
			}
		});
		
		for (Container c : peerList)
		{
			if (c.ttl <= 0 || serviced.contains(c.IPAddr.toString() + c.port))
			{
				toRemove.add(c);
			}
			serviced.add(c.IPAddr.toString() + c.port);
		}
		peerList.removeAll(toRemove);
	}
	
	public void agePeers()
	{
		for (Container c : peerList)
		{
			c.ttl--;
		}
	}

	private List<Container> parseArray(byte[] data) throws Exception
	{
		if (data.length % CONTAINER_SIZE != 0)
			throw new Exception("data.length must be multiple of " + CONTAINER_SIZE);

		List<Container> l = new ArrayList<Container>(data.length);
		for (int c = 0; c < data.length;)
		{
			Container container = new Container();
			
			byte[] IPAddr = new byte[4];
			IPAddr[0] = data[c++];
			IPAddr[1] = data[c++];
			IPAddr[2] = data[c++];
			IPAddr[3] = data[c++];
			
			container.IPAddr = InetAddress.getByAddress(IPAddr);
			container.port = (short)((data[c++] << 8) | data[c++]);
			container.ttl = (short)((data[c++] << 8) | data[c++]);
			l.add(container);
		}

		return l;
	}
}
