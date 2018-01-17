
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The system only accepts input format as "v:v,v,v".
 * File path is hard coded.
 * Port: 4500:Master-Worker, 4600:Worker-Worker.
 * The scenario is such: We have 10 servers, but only id:4 to id:10 can be workers.
 */
public class GraphProcessor {
	//Vertices
	ArrayList<Vertex> vertices_list = new ArrayList<Vertex>();//DEPRECATED
	HashMap<Integer, Vertex> vertices = new HashMap<Integer, Vertex>();
	
	//Memberships
	MembershipManager membership;//not synchronized for this app
	ArrayList<String> members;//only up to 7 workers
	String master = "172.22.146.152";///
	
	//Barrier
	int workerFinished = 0;
	final Lock lock = new ReentrantLock();
	final Condition barrier = lock.newCondition();
	
	//Sockets
	ServerSocket serverOne;
	ServerSocket serverTwo;
	Socket clientFromMaster;
	Socket clientToMaster;
	ArrayList<Socket> workerSockets;
	
	//Threads
	ArrayList<Thread> wtwThreads = new ArrayList<Thread>();
	ArrayList<Thread> mtwThreads = new ArrayList<Thread>();
	Thread masterSend;
	Thread masterReceive;
	Thread talkToMaster;
	Thread talkToWorker;
	
	//Application
	int iteration;
	Class cl = null;
	Constructor cons = null;
	int numWorker;
	double initValue;
	
	public GraphProcessor(MembershipManager membership){
		this.membership = membership;
		members = new ArrayList<String>();
		workerSockets = new ArrayList<Socket>();
		try {
			serverOne = new ServerSocket(4500);
			serverTwo = new ServerSocket(4600);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(!membership.ip.equals(master)){
			System.out.println("worker initialized");
			talkToMaster = new Thread(new Runnable(){
				public void run(){
					try {
						clientFromMaster = serverOne.accept();//what if master fails?
						clientToMaster = new Socket(master, 4500);
						DataInputStream in = new DataInputStream(clientFromMaster.getInputStream());
						DataOutputStream out = new DataOutputStream(clientToMaster.getOutputStream());	
						String msg = null;
						while(true){	
							msg = in.readUTF();
							//vertex type
							if(msg.charAt(0) == '#'){								
								String[] str = msg.trim().split(":");
								cl = Class.forName(str[0].substring(1));
								cons = cl.getConstructor(int.class, double.class);//generic?
								initValue = Double.valueOf(str[1]);
								numWorker = Integer.valueOf(str[2]);
								initWorker();
							}
							//parse input
							if(msg.charAt(0) == '?'){
								//System.out.println(msg);
								parse(msg.substring(1));
							}
							//start to work
							if(msg.charAt(0) == '!'){
								work();
								out.writeUTF("Finished");
								out.flush();
							}
							//prepare to work
							if(msg.charAt(0) == '*'){
								for(Vertex v : vertices.values()){///clear msg!
									v.clearMsg();
								}
								out.writeUTF("Cleared");
							}
							//if finished sending graph
							if(msg.equals("$")){
								System.out.println(msg);
								out.writeUTF("Parsed");
								out.flush();
							}
							//if finished iterations
							if(msg.equals("$$")){
								writeLog();
								System.out.println("Log finished!");
							}
						}
					} catch (IOException | ClassNotFoundException | NoSuchMethodException | SecurityException e) {
						e.printStackTrace();
					}
				}
			});
			talkToMaster.start();
		}
		
	}
	
	public void setIteration(int iteration){
		this.iteration = iteration;
	}
	
	/**
	 * Make TCP connection to all workers.
	 */
	public void init(){
		for(int i = (10 - numWorker); i < membership.membership.size(); i++){
			members.add(membership.membership.get(i));
		}
		for(String dest : members){
			//if(dest.split("_")[0].equals(membership.ip))//important!!
				//continue;
			Socket socket;
			try {
				if(membership.ip.equals(master))
					socket = new Socket(dest.split("_")[0], 4500);
				else
					socket = new Socket(dest.split("_")[0], 4600);
				workerSockets.add(socket);
			} catch (IOException e) {
				e.printStackTrace();
			}///
		}
		System.out.println("Connected to all workers: " + workerSockets.size());
	}
	
	public void initMaster(String vertexType){
		init();
		for(Socket s : workerSockets){
			try {
				DataOutputStream out = new DataOutputStream(s.getOutputStream());
				out.writeUTF("#" + vertexType + ":" + initValue + ":" + numWorker);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		partition();
		
		masterSend = new Thread(new Runnable(){
			public void run(){
				long start = System.currentTimeMillis();
				int i = 0;
				while(i < iteration){					
					try {					
						tellAllWorkers("!");
						synchronized(barrier){
							while(workerFinished != members.size()){
								barrier.wait();	
							}
							//System.out.println("all worker finished");
							workerFinished = 0;
						}
						//prepare
						tellAllWorkers("*");
						synchronized(barrier){
							while(workerFinished != members.size()){
								barrier.wait();	
							}
							//System.out.println("all worker prepared");
							workerFinished = 0;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println("iteration" + i + "finished");
					i++;
				}
				long end = System.currentTimeMillis();
				System.out.println("Running time: " + (end - start));
				tellAllWorkers("$$");
			}
		});
		masterReceive = new Thread(new Runnable(){
			public void run(){
				while(true){
					try {
						Socket masterForWorker = serverOne.accept();
						System.out.println("worker connected:" + masterForWorker);
						
						Thread t = new Thread(new Runnable(){
							Socket s = masterForWorker;
							DataInputStream in = new DataInputStream(s.getInputStream());
							public void run(){
								String msg = null;
								while(true){
									try {
										msg = in.readUTF();
										if(msg.equals("Parsed")){
											workerFinished++;
											//System.out.println("one worker finished parsing");
											if(workerFinished == members.size()){
												workerFinished = 0;
												masterSend.start();
											}
										}
										if(msg.equals("Finished")){
											workerFinished++;
											if(workerFinished == members.size()){
												synchronized(barrier){
													barrier.notify();
												}
											}
										}
										if(msg.equals("Cleared")){
											workerFinished++;
											if(workerFinished == members.size()){
												synchronized(barrier){
													barrier.notify();
												}
											}
										}
									} catch (IOException e) {
										e.printStackTrace();
									}
								}
							}	
						});
						
						mtwThreads.add(t);
						t.start();	
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		});
		masterReceive.start();
	}
	
	public void initWorker(){
		init();
		talkToWorker = new Thread(new Runnable(){
			public void run(){
				try {
					while(true){
						Socket clientForWorker = serverTwo.accept();
						System.out.println("worker connected:" + clientForWorker);
						
						Thread t = new Thread(new Runnable(){
							Socket s = clientForWorker;
							DataInputStream in = new DataInputStream(s.getInputStream());
							public void run(){
								String msg = null;
								while(true){
									try {
										msg = in.readUTF();
										updateValue(msg);
									} catch (IOException e) {
										e.printStackTrace();
									}			
								}
							}
						});
						
						wtwThreads.add(t);
						t.start();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		talkToWorker.start();	
	}
	
	
	//clean up code
	public void startOver(){
		
	}
	
	/**
	 * Partition graph and send it to workers. 
	 * Record the loading time.
	 */
	public void partition(){
		long start = System.currentTimeMillis();
		File file = new File("./graph.txt");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = reader.readLine()) != null){
				//based on id % N
				int v_id = Integer.valueOf(line.split(":")[0]);
				int place = v_id % members.size();
				Socket s = workerSockets.get(place);
				DataOutputStream out = new DataOutputStream(s.getOutputStream());
				out.writeUTF("?" + line);
				out.flush();
			}
			//tell all workers finished sending input
			tellAllWorkers("$");
			long end = System.currentTimeMillis();
			System.out.println("Loading time:" + (end - start));
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	//if this is a worker
	
	/**
	 * Compute() function for a worker.
	 */
	public void work(){
		for(Vertex v : vertices.values()){///clear msg!
			v.compute();
		}
		System.out.println("Finished current iteration");
	}	
	
	/**
	 * Parse graph sent from master. Initialize vertex instances.
	 * @param msg
	 */
	public void parse(String msg){
		String[] colon_msg = msg.split(":");
		String[] comma_msg = colon_msg[1].split(",");
		try {
			int v_id = Integer.valueOf(colon_msg[0]);
			Vertex v = (Vertex) cons.newInstance(v_id, initValue);
			int id = members.indexOf(membership.id);
			v.setMachineId(id);
			v.setWorkers(members);
			v.setSockets(workerSockets);
			vertices.put(v_id, v);
			for(String s : comma_msg){
				int neighbor_id = Integer.valueOf(s);
				if(!vertices.containsKey(neighbor_id)){
					Vertex vv = (Vertex) cons.newInstance(neighbor_id, initValue);
					vertices.put(neighbor_id, vv);
					v.addNode(vv);
				}else{
					Vertex vv = vertices.get(neighbor_id);
					v.addNode(vv);
				}
			}
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Receive message from other workers and push it to the message queue.
	 * @param msg
	 */
	public void updateValue(String msg){
		//System.out.println("received: " + msg);
		String[] message = msg.split(":");
		Vertex v = vertices.get(Integer.valueOf(message[0]));
		v.push(Double.valueOf(message[1]));//no generic
	}
	
	/**
	 * Helper function. Send message to all workers.
	 * @param msg
	 */
	public void tellAllWorkers(String msg){
		for(Socket s : workerSockets){
			try {
				DataOutputStream out = new DataOutputStream(s.getOutputStream());
				out.writeUTF(msg);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void findTop(){
		for(Vertex v : vertices.values()){
			
		}
	}
	
	public void writeLog(){
		try {
			PrintWriter writer = new PrintWriter("output.txt");
			for(Vertex v : vertices.values()){
				writer.println(v.getId() + ":" + v.getValue());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * This function is to parse the data from Amazon. The system only accepts input format as "v:v,v,v".
	 * NOT INTENDED FOR USERS.
	 * @param vertexType Specify the type the vertex.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 */
	public void parseRaw(String vertexType) throws IOException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		Class<?> cl = Class.forName(vertexType);
		Constructor<?> cons = cl.getConstructor(int.class, double.class);
		File file = new File("/Users/sylvester/Documents/workspace2/425mp4/src/API/com-amazon.ungraph.txt");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		HashMap<Integer, Vertex> map = new HashMap<Integer, Vertex>();
		while((line = reader.readLine()) != null){
			Scanner scanner = new Scanner(line);
			int fromNode = scanner.nextInt();
			int toNode = scanner.nextInt();
			Vertex fromVertex = null;
			Vertex toVertex = null;
			if(!map.containsKey(fromNode)){
				fromVertex = (Vertex)cons.newInstance(fromNode, 1);
				map.put(fromNode, fromVertex);
				vertices_list.add(fromVertex);
			}else{
				fromVertex = map.get(fromNode);
			}
			if(!map.containsKey(toNode)){
				toVertex = (Vertex)cons.newInstance(toNode, 1);
				map.put(toNode, toVertex);
				vertices_list.add(toVertex);
			}else{
				toVertex = map.get(toNode);
			}
			fromVertex.addNode(toVertex);
			toVertex.addNode(fromVertex);
		}
		
		PrintWriter writer = new PrintWriter("/Users/sylvester/Documents/workspace2/425mp4/src/API/graph.txt");
		for(Vertex v : vertices_list){
			writer.print(v.getId() + ":");
			ArrayList<Vertex> list = v.getNeighbors();
			for(int i = 0; i < list.size(); i++){
				writer.print(list.get(i).getId());
				if(i != list.size() - 1)
					writer.print(",");
			}
			writer.println();
		}
		writer.flush();
		writer.close();
		System.out.println("Finished parsing the graph");		
	}
	
}



