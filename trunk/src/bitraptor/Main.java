package bitraptor;

import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import org.klomp.snark.bencode.*;

public class Main
{
	private static byte[] protocolName = {'B', 'i', 't', 'T', 'o', 'r', 'r', 'e', 'n', 't', ' ', 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l'};
	
	private static ServerSocketChannel sock;
	private static Selector select;
	private static HashMap<String, Torrent> torrents = new HashMap<String, Torrent>();
	private static HashMap<SocketChannel, ByteBuffer> buffers = new HashMap<SocketChannel, ByteBuffer>();

	private static int port = 45507;

	/**
		Starts the BitRaptor program.  No arguments required.
	 */
	public static void main(String[] args) throws Exception
	{
		System.out.println("BitRaptor -- A bittorrent client");
		System.out.println("(Type 'help' to see available commands)");

		if ((args == null) || (args.length == 0))
		{
			System.out.println("Usage: BitRaptor <port>");
			return;
		}

		port = new Integer(args[0]);

		if (port < 1024 || port > 65535)
		{
			System.out.println("ERROR: Invalid port number, choose one between 1024-65535");
			return;
		}
		
		//Starting up the main socket server (not blocking) to listen for incoming peers
		try
		{
			select = Selector.open();
			sock = ServerSocketChannel.open();
			sock.configureBlocking(false);
			sock.socket().setReuseAddress(true);
			sock.socket().bind(new InetSocketAddress(port));
		}
		catch (Exception e)
		{
			sock = null;
			System.out.println("ERROR: Could not open up socket server, will not receive incoming connections from peers.");
		}
			
		//Preparing to read user input
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String[] command;
		System.out.print("> ");
		
		//Preparing to accept incoming peer connections
		SocketChannel incSock;
		
		//Handling user input and socket server interactions
		while (true)
		{
			//Process any new user input
			try
			{
				if (in.ready())
				{
					//Executing the specified command
					command = in.readLine().trim().split(" ");
			
					//Help
					if (command[0].equalsIgnoreCase("help"))
					{
						(new Main()).handleHelp();
					}
					//Exit
					else if (command[0].equalsIgnoreCase("exit") || command[0].equalsIgnoreCase("quit") || command[0].equalsIgnoreCase("bye"))
					{
						System.exit(0);
					}
					//Download
					else if (command[0].equalsIgnoreCase("download") || command[0].equalsIgnoreCase("dl") || command[0].equalsIgnoreCase("steal"))
					{
						//No torrent file specified
						if (command.length != 2 || command[1] == null)
						{
							System.out.println("Usage: " + command[0] + " <Torrent File>");
						}
						else
						{
							(new Main()).handleDownload(new File(command[1]));
						}
					}
					else if (command[0].equalsIgnoreCase("createring") || command[0].equalsIgnoreCase("cr"))
					{
						new RaptorRing(port).start();
					}
					//Invalid Command
					else
					{
						System.out.println("Invalid command. Type 'help' to see available commands.");
					}
			
					//Prompting again for user input
					System.out.print("> ");
				}
			}
			catch (IOException e)
			{
			}
			
			//Accept any new incoming peer connections
			try
			{
				if (sock != null && (incSock = sock.accept()) != null)
				{
					incSock.configureBlocking(false);
					incSock.register(select, SelectionKey.OP_READ);

					buffers.put(incSock, ByteBuffer.allocate(68));
					
//					System.out.println("[INC] " + (InetSocketAddress)(incSock.socket().getRemoteSocketAddress()));
				}
			}
			catch (Exception e)
			{
			}
			
			//Accept handshakes on any incoming peer connections
			try
			{
				//Performing the select
				//select.selectNow();
				select.select(100);
				Iterator it = select.selectedKeys().iterator();
				
				while(it.hasNext())
				{
					SelectionKey selected = (SelectionKey)it.next();
					it.remove();

					//Handling the read
					if (selected.isReadable())
					{
						incSock = (SocketChannel)selected.channel();
						ByteBuffer buffer = buffers.get(incSock);

						//Reading from the socket and continuing on if not at end of stream / full buffer
						incSock.read(buffer);
						if (buffer.hasRemaining())
						{
							continue;
						}

						buffer.flip();

						//Dropping the connection if invalid name length
						if (buffer.get() != 19)
						{
							selected.cancel();
							buffers.remove(incSock);
							incSock.close();
							continue;
						}

						//Dropping the connection if invalid protocol name
						byte[] name = new byte[19];
						buffer.get(name);
						for (int b = 0; b < 19; b++)
						{
							if (protocolName[b] != name[b])
							{
								selected.cancel();
								buffers.remove(incSock);
								incSock.close();
								continue;
							}
						}

						//Skipping over the next 8 reserved bytes
						buffer.getDouble();

						//Getting the info hash and peer ID
						byte[] infoHash = new byte[20];
						byte[] peerID = new byte[20];
						buffer.get(infoHash);
						buffer.get(peerID);

						//Checking to make sure a current torrent matches it
						if (torrents.containsKey(new String(infoHash)))
						{
							Torrent torrent = ((Torrent)torrents.get(new String(infoHash)));

//							System.out.println("[INC HANDSHAKE] " + (InetSocketAddress)(incSock.socket().getRemoteSocketAddress()));

							//Giving the peer to the torrent to handle
							selected.cancel();
							torrent.addPeer(new Peer(torrent, peerID, incSock), true);
						}
						//Dropping the connection if no torrent matches it
						else
						{
//							System.out.println("[INC FAIL] " + (InetSocketAddress)(incSock.socket().getRemoteSocketAddress()));
							selected.cancel();
							buffers.remove(incSock);
							incSock.close();
							continue;
						}
					}
				}
			}
			catch (Exception e)
			{
//				System.out.println("Exception: " + e);
//				e.printStackTrace();
			}

			//Sleep for 50 ms since this loop does not require high speed
			Thread.sleep(50);
		}
	}
	
	/**
		Helper function for the "help" command used by main()
	*/
	private void handleHelp()
	{
		String str = "";
		str += "Available commands are:\n";
		str += "\tdownload <Torrent File> -- Downloads the files associated with the torrent [NOTE: No spaces in file name]\n";
		str += "\tcreatering -- Enables bitsecant\n";
		str += "\texit -- Exits the BitRaptor program\n";
		
		System.out.println(str);
	}
	
	/**
		Helper function for the "download" command used by main().
		Assumes that file != null.  If file is null, just returns without doing anything.
		
		@param file - The torrent file to read from
	*/
	private void handleDownload (File file)
	{
		//Checking if the torrent file exists, is a file, and is readable
		if (file == null)
		{
			return;
		}
		else if (!file.exists())
		{
			System.out.println("ERROR: " + file.getAbsolutePath() + " does not exist.");
			return;
		}
		else if (!file.isFile())
		{
			System.out.println("ERROR: " + file.getAbsolutePath() + " is not a valid file.");
			return;
		}
		else if (!file.canRead())
		{
			System.out.println("ERROR: " + file.getAbsolutePath() + " is not open for read access.");
			return;
		}
		
		//Begin parsing the torrent file
		BDecoder decoder = null;
		BEValue value = null;
		Info info = new SingleFileInfo(); //declare as a singlefile info, change later
		
		try
		{
			decoder = new BDecoder(new FileInputStream(file));
		}
		catch (FileNotFoundException e)
		{
			System.out.println("ERROR: " + file.getAbsolutePath() + " does not exist.");
			return;
		}
		
		try
		{
			Map<String, BEValue> torrentFileMappings = decoder.bdecode().getMap();
			Set<String> fields = torrentFileMappings.keySet();
			
			//Handling each field in the file
			for (String field : fields)
			{
				//Info
				if (field.equalsIgnoreCase("info"))
				{
					
					Map<String, BEValue> infoDictionary = torrentFileMappings.get(field).getMap();

					//Multiple Files Mode
					if (infoDictionary.containsKey("files"))
					{
						info = new MultiFileInfo(info);
						MultiFileInfo infoAlias = (MultiFileInfo)info;
						
						infoAlias.setDirectory(new String(infoDictionary.get("name").getBytes()));
						infoAlias.setFiles(new ArrayList<SingleFileInfo>());
						
						List<BEValue> fileDictionaries = infoDictionary.get("files").getList();
						LinkedList<SingleFileInfo> files = new LinkedList<SingleFileInfo>();
						for (BEValue fileDictionary : fileDictionaries)
						{
							Map<String, BEValue> fileDictionaryMap = fileDictionary.getMap();
							SingleFileInfo fileInfo = new SingleFileInfo();

							//Name and path
							List<BEValue> paths = fileDictionaryMap.get("path").getList();
							String filePath = null;
							
							for (int c = 0; c < paths.size(); c++)
							{
								if (c == 0)
									filePath = new String(paths.get(c).getBytes());
								else
									filePath += "/" + new String(paths.get(c).getBytes());
							}
							
							fileInfo.setName(filePath);

							//File size
							fileInfo.setFileLength(fileDictionaryMap.get("length").getInt());

							//MD5 checksum
							if (fileDictionaryMap.containsKey("md5sum"))
							{
								fileInfo.setMd5sum(fileDictionaryMap.get("md5sum").getBytes());
							}

							//Adding the file to the directory
							files.add(fileInfo);
						}

						infoAlias.setFiles(files);
					}
					//Single File Mode
					else if (infoDictionary.containsKey("length"))
					{
						info = new SingleFileInfo(info);
						SingleFileInfo infoAlias = (SingleFileInfo)info;

						//Name
						infoAlias.setName(new String(infoDictionary.get("name").getBytes()));

						//Length
						infoAlias.setFileLength(infoDictionary.get("length").getInt());

						//MD5 Checksum
						if (infoDictionary.containsKey("md5sum"))
						{
							infoAlias.setMd5sum(infoDictionary.get("md5sum").getBytes());
						}
					}
					else
						throw new Exception("Invalid torrent file.  info doesn't contain files or length");
					
					//Hash of 'info' field
					info.setInfoHash(decoder.get_special_map_digest());

					//Pieces
					info.setPieces(infoDictionary.get("pieces").getBytes());

					//Piece Length
					info.setPieceLength(infoDictionary.get("piece length").getInt());

					//Private
					if (infoDictionary.containsKey("private"))
					{
						info.setPrivateTorrent((infoDictionary.get("private").getInt() == 1) ? true : false);
					}
					else
					{
						info.setPrivateTorrent(false);
					}
				}
				//Nodes
				else if (field.equalsIgnoreCase("nodes"))
				{
					List<BEValue> nodesList = torrentFileMappings.get(field).getList();
					Map<InetAddress, Integer> nodes = new HashMap<InetAddress, Integer>();
					for (BEValue nodeList : nodesList)
					{
						List<BEValue> nodeInfoList = nodeList.getList();
						
						String address = nodeInfoList.get(0).getString();
						int port = nodeInfoList.get(1).getInt();
						
						try
						{
							nodes.put(InetAddress.getByName(address), port);
						}
						catch (Exception e)
						{
							
						}
					}
					
					info.setNodes(nodes);
				}
				//Creation Date
				else if (field.equalsIgnoreCase("creation date"))
				{
					info.setCreationDate(torrentFileMappings.get(field).getLong());
				}
				//Comment
				else if (field.equalsIgnoreCase("comment"))
				{
					info.setComment(new String(torrentFileMappings.get(field).getBytes()));
				}
				//Created-By
				else if (field.equalsIgnoreCase("created by"))
				{
					info.setCreatedBy(new String(torrentFileMappings.get(field).getBytes()));
				}
				//Encoding
				else if (field.equalsIgnoreCase("encoding"))
				{
					info.setEncoding(new String(torrentFileMappings.get(field).getBytes()));
				}
			}
		}
		//Invalid torrent file (Could not be parsed)
		catch (Exception e)
		{
			System.out.println("ERROR: Invalid torrent file");
			e.printStackTrace();
			return;
		}
		
		////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////
		System.out.println("Nodes: " + info.getNodes());
		System.out.println("Torrent Created By " + info.getCreatedBy() + " on " + info.getCreationDate() +" with encoding " + info.getEncoding());
		System.out.println("Comments: " + info.getComment());
		System.out.println("Info:");
		System.out.println("\tPiece Length : " + info.getPieceLength());
		System.out.println("\tPrivate: " + info.isPrivateTorrent());
		System.out.println(info.toString());
		////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////
		
		//Starting a new thread for the torrent
		final Info torrentInfo = info;
		new Thread(new Runnable()
		{
			public void run()
			{
				try
				{
					Torrent torrent = new Torrent(torrentInfo, port);
		
				torrents.put(new String(torrentInfo.getInfoHash()), torrent);
				
				torrent.start();
				}
				catch (Exception e)
				{
					//donothing
				}
			}
		}).start();
	}
}
