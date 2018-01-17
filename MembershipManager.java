import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

public class MembershipManager {
	
	LinkedList<String> membership;//full membership list
	LinkedList<String> neighbors;//successors and predecessors
	HashMap<String, Long> membershipMap;//timestamps of neighbors for detecting failures, format <Key,value> = <id,timestamp>
	String ip;
	String timestamp;
	String id;
	DatagramSocket socket;//for dissemination
	DatagramPacket packet; 	
	Thread intro;
	Thread send;
	Thread receive;
	Thread detect;
	Thread churn;
	FileManager fm;


	public MembershipManager() throws SocketException{
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
			timestamp = new Date().toString();
			id = ip +"_"+ timestamp;
			socket = new DatagramSocket(4234);
			membership = new LinkedList<String>();
			neighbors = new LinkedList<String>();
			membershipMap = new HashMap<String,Long>();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * Start the protocol
	 */
	public void run(){
		send = new Thread(new Runnable(){
			public void run() {
				send();
			}
		});	
		receive = new Thread(new Runnable(){
			public void run() {
				receive();
			}
		});		
		detect = new Thread(new Runnable(){
			public void run() {
				detect();
			}
		});
		churn = new Thread(new Runnable(){
			public void run() {
				try {
					churn();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		send.start();
		receive.start();
		detect.start();
		churn.start();
	}
	
	
	/**
	 * The introducer must join first
	 */
	public void join(){
		if(ip.equals("172.22.146.152")){//introducer fixed to the first machine
			System.out.println("Introducer joins");
			membership.add(id);
			intro = new Thread(new Runnable(){
				public void run(){
					introduce();
				}
			});
			intro.start();
		}else{
			try {
				String msg = "join" + "," + id;
				DatagramSocket socket = new DatagramSocket();
				DatagramPacket pSend = new DatagramPacket(msg.getBytes(), msg.getBytes().length, InetAddress.getByName("172.22.146.152"), 4233);
				socket.send(pSend);
				
				//wait for the membership list
				byte[] bytes = new byte[4096];
				DatagramPacket pReceive = new DatagramPacket(bytes, bytes.length);
				socket.receive(pReceive);
				String result = new String(pReceive.getData(), 0, pReceive.getData().length).trim();
				for(String s : result.split(",")){
					s = s.trim();
					membership.add(s);
					membershipMap.put(s, System.currentTimeMillis());
				}
				neighbors = getNeighbors();
				
				System.out.println("Successfully join to group: " + id);
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * The method for introducer
	 */
	public void introduce(){
		byte[] bytes = new byte[1024];
		DatagramSocket socket;
		DatagramPacket packet; 
		try {
			socket = new DatagramSocket(4233);
			packet = new DatagramPacket(bytes, bytes.length);
			while(true){		
				try {
					socket.receive(packet);
					String msg = new String(packet.getData(), 0, packet.getData().length).trim();//must parse this way
					String id = msg.split(",")[1];
					System.out.println("A server ask for join: " + id);
					
					synchronized(membershipMap){
						membershipMap.put(id, System.currentTimeMillis());
					}
					String ret = "";
					synchronized(membership){
						membership.add(id);				
						for(int i = 0; i < membership.size(); i++){
							ret += membership.get(i) + ",";
						}
					}
					synchronized(neighbors){
						neighbors = getNeighbors();
					}
					
					//send membership list to the new joined node
					ret = ret.substring(0, ret.length() - 1);
					DatagramPacket pSend = new DatagramPacket(ret.getBytes(), ret.getBytes().length, packet.getAddress(), packet.getPort());
					socket.send(pSend);
					disseminate(msg);	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (SocketException e1) {
			e1.printStackTrace();
		}
		
		
	}
	
	/**
	 * Send the heartbeat to neighbors
	 */
	public void send() {
		DatagramSocket socket;
		try {
			socket = new DatagramSocket();
			byte[] bytes = new byte[1024];
			DatagramPacket packet;
			while(true){
				synchronized(neighbors){
					for(String neighbor : neighbors){
							InetAddress receiver;
							receiver = InetAddress.getByName(neighbor.split("_")[0]);
							String heartbeatInfo = this.id;
							packet = new DatagramPacket(heartbeatInfo.getBytes(), heartbeatInfo.getBytes().length, receiver, 4231);
							socket.send(packet);
							//System.out.println("Send heartbeat!");
					}
				}
				Thread.sleep(100);//Send every 100ms
			}
		} catch (IOException | InterruptedException e1) {
			e1.printStackTrace();
		}		
	}
	
	/**
	 * Receive heartbeat from neighbors
	 */
	public void receive(){
		try {
			DatagramSocket socket = new DatagramSocket(4231);
			byte[] bytes = new byte[1024];
			DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
			while(true){
				socket.receive(packet);
				synchronized(membershipMap){	
					String msg = new String(packet.getData(), 0, packet.getData().length).trim();
					long last = membershipMap.get(msg);
					long curr = System.currentTimeMillis();
					if(curr > last)
						membershipMap.put(msg, curr);
					//System.out.println("Received heartbeat: " + msg);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Check every 2 second the membershipMap to see if there's any failure
	 */
	public void detect(){
		while(true){
			LinkedList<String> lost = new LinkedList<String>();
			synchronized(neighbors){
				synchronized(membershipMap){
					
					for(String neighbor : neighbors){
						long lastTime = membershipMap.get(neighbor);//will the lookup have race condition?
						long currTime = System.currentTimeMillis();
						if(currTime - lastTime > 2000){
							lost.add(neighbor);
							System.out.println(currTime);
							System.out.println(lastTime);
							System.out.println("Times out: " + neighbor);
							String msg = "crash" + "," + neighbor;
							disseminate(msg);//tell all other machines
							membership.remove(neighbor);
							membershipMap.remove(neighbor);
							neighbors = getNeighbors();//update membership list
							//remove map?
						}
					}
				}
			}
			try {
				for(String l : lost){
					fm.replica(l);
					
				}
				Thread.sleep(500);
			} catch (InterruptedException | IOException | NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Deal with join or leave. 
	 * @throws InterruptedException 
	 */
	public void churn() throws InterruptedException{
		try {
			DatagramSocket socket = new DatagramSocket(4232);
			byte[] bytes = new byte[1024];
			DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
			String crashId = null;
			while(true){
				socket.receive(packet);
				boolean needReplicate = false;
				synchronized(neighbors){
					synchronized(membershipMap){
						String msg = new String(packet.getData(), 0, packet.getData().length).trim();
						String op = msg.split(",")[0];
						String id = msg.split(",")[1];
						System.out.println(msg);
						if(op.equals("join")){
							membership.add(id);
							membershipMap.put(id, System.currentTimeMillis());
							neighbors = getNeighbors();
						}else if(op.equals("leave") && membership.contains(id)){
							membership.remove(id);
							neighbors = getNeighbors();//remove map or not?
							membershipMap.remove(id);							
						}else if(op.equals("crash") && membership.contains(id)){
							membership.remove(id);
							neighbors = getNeighbors();
							membershipMap.remove(id);
							crashId = id;
							needReplicate = true;							
						}
					}
				}
				if(needReplicate){
					//Send file manager to replica
					System.out.println("HASH ID IS :"+fm.findFileHash(crashId));
					fm.replica(crashId);
				}
			}
		} catch (IOException | NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This is a one-to-all dissemination.
	 */
	public void disseminate(String msg){
		try {
			//send msg to all members
			for(int i = 0; i < membership.size(); i++){
				String id = membership.get(i).trim();
				if(!id.equals(this.id.trim()) && !id.equals(msg.split(",")[1])){
					packet = new DatagramPacket(msg.getBytes(), msg.getBytes().length, InetAddress.getByName(id.split("_")[0]), 4232);
					socket.send(packet);
					System.out.println("disseminate to: " + id);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println("Send message to others that "+id+" is down.");
		
	}
	
	/**
	 * This function will be called every time there's a churn.
	 */
	public LinkedList<String> getNeighbors() {
		LinkedList<String> result = new LinkedList<String>();
		System.out.println(id);
		//first find the index, later can use a global index
		int size = membership.size();
		int index = 0;
		for(int i = 0; i < size; i++){
			if(membership.get(i).equals(id)){
				index = i;
				break;
			}
		}
		//int index = membership.indexOf(id);		
		System.out.println(index + " " + size);
		//successor
		for(int i = 1; i <= 2; i++){
			int tmp = index + i;
			while(tmp >= size)
				tmp -= size;
			if(tmp != index && !result.contains(membership.get(tmp))){
				result.add(membership.get(tmp));
			}
		}
		//predecessor
		for(int i = 1; i <= 2; i++){
			int tmp = index - i;
			while(tmp < 0)
				tmp += size;
			if(tmp != index && !result.contains(membership.get(tmp))){
				result.add(membership.get(tmp));
			}
		}
		return result;
	}
	
	public void leave(){
		String msg = "leave" + "," + id;
		System.out.println("Asking for a leave: " + id);
		disseminate(msg);
		
		//clean up code
		send.interrupt();
		receive.interrupt();
		detect.interrupt();
		churn.interrupt();
		socket.close();
		membership = null;//should synchronize
		neighbors = null;
		membershipMap = null;	
	}
	
	//below are the APIs for demo
	public void printList(){
		System.out.println("Membership list of: "+ id);
		for(String member : membership){
			System.out.println(member);
		}
	}
	
	public void printNeighbor(){
		System.out.println("Neighbor of: "+ id);
		for(String neighbor : neighbors){
			System.out.println(neighbor);
		}
	}
	
	public void printId() {
		System.out.println("Id: "+ id);
	}
	
	public FileManager getFm() {
		return fm;
	}

	public void setFm(FileManager fm) {
		this.fm = fm;
	}

}
