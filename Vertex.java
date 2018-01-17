

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

/**
 * Graph processing applications must extends this class and overrides the compute().
 * @param <V> VertexValue
 * @param <E> EdgeValue
 * @param <M> MessageValue
 */
public abstract class Vertex<V,E,M> {
	
	int id;
	V value;
	ArrayList<Vertex> neighbors;
	Stack<M> messages;
	Stack<M> current_messages;
	
	//for distributed computing
	int machine_id;
	ArrayList<String> workers;
	ArrayList<Socket> sockets;
	
	public Vertex(int id, V value){
		this.id = id;
		this.value = value;
		neighbors = new ArrayList<Vertex>();
		messages = new Stack<M>();
		current_messages = new Stack<M>();
	}
	
	public void sendMessageTo(Vertex v, M msg){
		int hash = v.getId() % workers.size();
		//System.out.println(machine_id);
		//System.out.println(hash);
		if(hash == machine_id){
			v.push(msg);
		}else{
			//if vertex is in other machine, use socket
			//msg format: id:value
			try {
				Socket s = sockets.get(hash);
				DataOutputStream out = new DataOutputStream(s.getOutputStream());
				//System.out.println(v.getId() + ":" + msg);
				out.writeUTF(String.valueOf(v.getId()) + ":" + String.valueOf(msg));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void sendMessageToAll(M msg){
		for(Vertex v : neighbors){
			sendMessageTo(v, msg);
		}
	}
	
	public abstract void compute();
	
	
	
	
	public int getId(){
		return id;
	}
	
	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}
	
	public ArrayList<Vertex> getNeighbors(){
		return neighbors;
	}
	
	public int getNeighborSize(){
		return neighbors.size();
	}

	public void push(M msg){
		current_messages.add(msg);
	}
	
	public Iterator<M> getMessagesItr(){
		return messages.iterator();
	}
	
	//when initializing the graph
	public void addNode(Vertex v){
		neighbors.add(v);
	}
	
	public void clearMsg(){
		messages = current_messages;
		current_messages = new Stack<M>();
	}
	
	//
	public void setWorkers(ArrayList<String> workers){
		this.workers = workers;
	}
	
	public void setMachineId(int id){
		machine_id = id;
	}
	
	public void setSockets(ArrayList<Socket> sockets){
		this.sockets = sockets;
	}
	
}








