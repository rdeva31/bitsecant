package chord;

public class Chord
{
	private HashMap<String, ChordData> data;
	private LinkedList<ChordNode> fingerTable;
	private ChordNode predecessor, successor;
	 
	public void join(Chord ringParticipant)
	{
		predecessor = null;
		sucessor = ringParticipant.findSuccessor(this); //LOL ring participant fuck off lol fail
	}
	
	//public ChordNode findSuccessor(Chord
}