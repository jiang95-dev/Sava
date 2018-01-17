import java.util.Iterator;

class PageRankVertex extends Vertex<Double,Void,Double> {
	
	public PageRankVertex(int id, double value){
		super(id, value);
	}
	
	public void compute() {
		//gather
		double sum = 0;
		Iterator<Double> messagesItr = getMessagesItr();
		while(messagesItr.hasNext()){
			sum += messagesItr.next();
		}
		
		//apply
		if(sum != 0)
			setValue(sum);
		
		//scatter
		double neighborSize = (double)getNeighborSize();
		double value = getValue();
		sendMessageToAll(value / neighborSize);
	}

}