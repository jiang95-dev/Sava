
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Scanner;

public class Server {
	ServerSocket server = null;
	Socket client = null;
	DataOutputStream out = null;
	DataInputStream in = null;	

	public static void main(String[] args) throws IOException, NoSuchAlgorithmException{

		Server server = new Server();
		Scanner in = new Scanner(System.in);
		MembershipManager membership = new MembershipManager();
		FileManager fileManager = new FileManager();
		GraphProcessor processor = new GraphProcessor(membership);
		
		//Parsing user input
		while(true){
			if(in.hasNextLine()){
				String userInput = in.nextLine();
				//Process graph
				if(userInput.equals("process")){
					System.out.println("How many workers?");
					userInput = in.nextLine();
					processor.numWorker = Integer.valueOf(userInput);
					System.out.println("How many iterations?");
					userInput = in.nextLine();
					processor.setIteration(Integer.valueOf(userInput));
					System.out.println("What is the initial value?");
					userInput = in.nextLine();
					processor.initValue = Double.valueOf(userInput);
					System.out.println("What kind of vertex?");
					userInput = in.nextLine();
					processor.initMaster(userInput);
				}
				
				//Print member list
				if(userInput.equals("list")){
					membership.printList();
				}
				//Print neighbors
				else if(userInput.equals("neighbor")){
					membership.printNeighbor();
				}
				//Print id
				else if(userInput.equals("id")){
					membership.printId();
				}
				//Join group
				else if(userInput.equals("join")){
					membership.join();
					membership.run();
				}
				//Leave group
				else if(userInput.equals("leave")){
					membership.leave();
				}
				
				
				//Put file
				else if(userInput.contains("put")){
					String[] command = userInput.split(" ");
					if(command.length == 3){
						String localFilePath = command[1];
						String sdfsFileName = command[2];
						fileManager.put(localFilePath, sdfsFileName);
					}else{
						System.out.println("Usage: put localFilePath sdfsFileName");
					}
				}
				//Get file
				else if(userInput.contains("get")){
					String[] command = userInput.split(" ");
					if(command.length == 3){
						String sdfsFileName= command[1];
						String localFilePath = command[2];
						fileManager.get(sdfsFileName, localFilePath);
					}else{
						System.out.println("Usage: get sdfsFileName localFilePath");
					}
				}
				//Delete file
				else if(userInput.contains("delete")){
					String[] command = userInput.split(" ");
					if(command.length == 2){
						String sdfsFileName= command[1];
						fileManager.delete(sdfsFileName);
					}else{
						System.out.println("Usage: delete sdfsFileName");
					}
				}
				//List which servers the file lies on
				else if(userInput.contains("list")){
					String[] command = userInput.split(" ");
					if(command.length == 2){
						String sdfsFileName= command[1];
						fileManager.list(sdfsFileName);
					}else{
						System.out.println("Usage: delete sdfsFileName");
					}
				}
				//List files on local machine
				else if(userInput.equals("store")){
					fileManager.store();
				}
			}

		}
	}

	public Server(){
		try {
			server = new ServerSocket(4241);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* 
	 *  Server will be blocked until it accept a client. Then it will be blocked
	 *  until it recieves a command. The it uses a helper function to execute the command.
	 */
	public void run(){
		try {
			client = server.accept();
			in = new DataInputStream(client.getInputStream());
			out = new DataOutputStream(client.getOutputStream());
			System.out.println("connected to client");
			while(true){		
				System.out.println("connected to client");
				String command = null;
				while(true){
					System.out.println("connected to client");
					command = in.readUTF();
					if(command != null)
						break;

				}
				doGrep(command);

			}

		} catch (IOException e) {
			System.out.println("client left");
		}
	}

	/*  
	 *  Create a process to run shell command. Then create a buffer reader to read the result.
	 *  Then send the result back to the client with newline suggesting the end of result.
	 */
	public void doGrep(String command){
		Process p;
		try {
			p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
			System.out.println("started to grep");
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				out.writeUTF(line);
			}
			out.writeUTF("\n");
		} catch (IOException e) {
			System.out.println("error caused by grep");
		}
	}

}
