package API;

import java.util.Iterator;

public class ShortestPathVertex extends Vertex<Double,Void,Double>{
	
	public ShortestPathVertex(int id, double value){
		super(id, value);
	}

	@Override
	public void compute() {
		Double mindist = Double.MAX_VALUE;
		//Check if the node is itself
		if(this.id == 245556){
			mindist = 0.0;
		}
		
		Iterator<Double> messagesItr = getMessagesItr();
		while(messagesItr.hasNext()){
			Double tempM = messagesItr.next();
			//System.out.println("Message "+tempM);
			if(tempM<mindist){
				mindist = tempM;
				//System.out.println("Set "+this.id+" temp M "+mindist);
				//System.out.println(getValue());
			}
		}
		clear();
		
		if(mindist < getValue()){
			setValue(mindist);
			for(Vertex v : this.neighbors){
				try {
					Thread.sleep(0);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				double tmp = 1;
				//System.out.println("Neighbor value is "+tmp);
				//System.out.println("Send message to "+v.id+" "+(tmp+mindist));
				sendMessageTo(v, tmp+mindist);
				try {
					Thread.sleep(0);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

}
