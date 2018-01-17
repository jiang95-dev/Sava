import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Vector;

public class FileManager {

	/********************************************************************************************
	 * 
	 * TO-DO: STORE(SAVE IT AFTER MP2 IS DONE)
	 * 
	 *********************************************************************************************/

	private final int transferPort = 5000;
	private final int serverPort = 5050;
	private final String LOCAL_ADDRESS = InetAddress.getLocalHost().getHostAddress();
	private ServerSocket fileReceiver;
	private ServerSocket fileHandler;
	private MembershipManager mm;
	private String tempFileName="";
	private boolean updateLock = false;





	/**
	 * Constructor of File Manager: Set up connections and init config
	 * @throws IOException 
	 * @throws JSchException
	 */
	FileManager() throws IOException{
		fileReceiver = new ServerSocket(transferPort);
		fileHandler = new ServerSocket(serverPort);

		//Thread to receive and send files, use transferPort = 5000
		Thread receiver =  new Thread(new Runnable(){
			public void run(){
				try {
					while(true){
						synchronized(fileReceiver){
							save();
						}
					}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		receiver.start();

		//Thread to handle file request: get, delete, etc, use serverPort = 5050
		Thread fileHandler =  new Thread(new Runnable(){
			public void run(){
				try {
					while(true)
						handler();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		fileHandler.start();
	}

	/**
	 * Function to send file to server side
	 * @param localPath
	 * @param serverPath
	 * @throws UnknownHostException 
	 * @throws SocketException
	 * @throws IOException
	 * @throws SftpException
	 * @throws JSchException
	 */
	public void put(String address, String localPath, String serverPath) throws UnknownHostException, IOException{

		//If the address is equal to itself, don't push it
		if(address.equals(InetAddress.getLocalHost().getHostAddress()) && new File("files/"+localPath).exists()){
			return;
		}	

		long startTime = System.currentTimeMillis();


		//Specify the file
		File file = new File("files/"+localPath);

		Socket socket = new Socket(InetAddress.getByName(address), transferPort);

		FileInputStream fis = new FileInputStream(file);
		BufferedInputStream bis = new BufferedInputStream(fis); 
		//Output stream
		OutputStream os = new DataOutputStream(socket.getOutputStream());
		//Read File Contents into contents array 
		byte[] contents;
		long current = 0;

		//Send file name first
		contents = (serverPath).getBytes();
		os.write(contents);
		os.flush(); 

		//Delay 1s...so there will be no bug...
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		while(current!=file.length()){ 
			int size = 10000;
			if(file.length() - current >= size)
				current += size;    
			else{ 
				size = (int)(file.length() - current); 
				current = file.length();
			} 
			contents = new byte[size]; 
			bis.read(contents, 0, size); 
			os.write(contents);
		}   

		os.flush(); 
		//File transfer done. Close the socket connection!
		socket.close();
		System.out.println("File "+serverPath+" sent to "+address+" succesfully!");

		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Running time: "+totalTime/1000.0+" sec.");


	}

	/**
	 * Function to save file on server side
	 * @throws IOException
	 */
	public void save() throws IOException{

		long start = System.nanoTime();

		Socket socket = fileReceiver.accept();

		byte[] contents = new byte[16];

		InputStream is = socket.getInputStream();	

		String fileName = new String(contents, 0, is.read(contents));


		File f = new File("files/"+fileName);
		//Check if the update is less than one minute
		if(updateLock == false){
			if(f.exists() && (System.currentTimeMillis()-f.lastModified() < 60000)){
				socket.close();
				updateLock = true;
				System.out.println("The last update of file "+fileName+" is less than one minute on this machine.");
				String  destination = socket.getRemoteSocketAddress().toString().replace("/","").split(":")[0];
				destination = destination.replace("/","");
				//				Socket socketT = new Socket(destination, serverPort);
				//				DataOutputStream outToServer = new DataOutputStream(socketT.getOutputStream());
				//				outToServer.writeBytes(("LIST:"+clientSentence.split(":")[1]+"(FILE NOT FOUND):"+InetAddress.getLocalHost().getHostAddress()) + '\n');
				//				socketT.close();
				//				System.out.println("Send back LIST NOT FOUND to "+clientSentence.split(":")[1]+":"+InetAddress.getLocalHost().getHostAddress());
				//				
				Socket socketT = new Socket(InetAddress.getByName(destination), serverPort);

				System.out.println(destination);
				OutputStream os = socketT.getOutputStream();

				contents = new byte[100];
				contents = ("UPDATE CONFIRMATION:"+LOCAL_ADDRESS+"#"+fileName).getBytes();
				os.write(contents);
				os.flush(); 
				//File transfer done. Close the socket connection!
				socketT.close();

				final String DEST = destination;
				//Set up a timer 
				Thread timer =  new Thread(new Runnable(){
					public void run(){
						try {
							Thread.sleep(30000);
							if(updateLock == true){
								//Send times out
								Socket socketT = new Socket(InetAddress.getByName(DEST), serverPort);
								byte[] contents = new byte[100];
								OutputStream os = socketT.getOutputStream();

								contents = new byte[100];
								contents = ("UPDATE TIME OUT:"+LOCAL_ADDRESS+"#"+fileName).getBytes();
								os.write(contents);
								os.flush(); 
								//File transfer done. Close the socket connection!
								socketT.close();
								updateLock = false;
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
				timer.start();
			}
			else{
				//FileOutputStream to the output file's path
				System.out.println("GENERATE FILE: "+fileName);

				FileOutputStream fos = new FileOutputStream("files/"+fileName);
				BufferedOutputStream bos = new BufferedOutputStream(fos);

				//No of bytes read in one read() call
				int bytesRead = 0; 

				while((bytesRead=is.read(contents))!=-1)
					bos.write(contents, 0, bytesRead); 

				bos.flush(); 
				socket.close(); 
				//System.out.println("File :"+fileName+" received succesfully!");
			}
		}
		//(updateLock == true)
		else{
			//FileOutputStream to the output file's path
			System.out.println("GENERATE FILE: "+fileName);
			updateLock = false;
			FileOutputStream fos = new FileOutputStream("files/"+fileName);
			BufferedOutputStream bos = new BufferedOutputStream(fos);

			//No of bytes read in one read() call
			int bytesRead = 0; 

			while((bytesRead=is.read(contents))!=-1)
				bos.write(contents, 0, bytesRead); 

			bos.flush(); 
			socket.close(); 
			System.out.println("File :"+fileName+" received succesfully!");

		}
	}

	/**
	 * Function to handle file transaction
	 * @throws IOException
	 * @throws NoSuchAlgorithmException 
	 */
	public void handler() throws IOException, NoSuchAlgorithmException{

		Socket socket = fileHandler.accept();

		BufferedReader inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		DataOutputStream outToClient = new DataOutputStream(socket.getOutputStream());

		String clientSentence;
		while((clientSentence = inFromClient.readLine()) != null){
			//Length check
			if(clientSentence.split(":").length<=1){
				System.out.println("RECEIVED: " + clientSentence);

				System.out.println("Bad request " + clientSentence);
			}
			//Handle Incoming file request
			else if(clientSentence.split(":").length==2){
				//Request from client to check if a file exist for get 
				if(clientSentence.split(":")[0].equals("REQUEST")){
					System.out.println("RECEIVED: " + clientSentence);
					String ack = clientSentence.split(":")[1];
					String severfileName = ack.split("#")[1];
					String localFileName = ack.split("#")[2];
					File f = new File("files/"+severfileName);
					//If file exists
					if(f.exists() && !f.isDirectory()) { 
						//Send back ack
						String destination = clientSentence.split(":")[1].split("#")[0].trim();
						Socket socketT = new Socket(destination, serverPort);
						DataOutputStream outToServer = new DataOutputStream(socketT.getOutputStream());
						outToServer.writeBytes(("HAVE:"+severfileName+"#"+localFileName+":"+LOCAL_ADDRESS) + '\n');
						socketT.close();
					}
				}
				else if(clientSentence.split(":")[0].equals("GET")){
					System.out.println("RECEIVED: " + clientSentence);
					String serverFileName = clientSentence.split(":")[1].split("#")[0];
					String localFileName = clientSentence.split(":")[1].split("#")[1];
					String destination = socket.getRemoteSocketAddress().toString().replace("/","").split(":")[0];
					put(destination,serverFileName,localFileName);
				}
				else if(clientSentence.split(":")[0].equals("UPDATE CONFIRMATION")){
					System.out.println("RECEIVED: " + clientSentence);

					String serverFileName = clientSentence.split(":")[1].split("#")[1];

					String destination = clientSentence.split(":")[1].split("#")[0];
					System.out.println("Update on "+destination+" of file :"+serverFileName+" is less than one minute.");
					System.out.println("Put the command again to comfirm the update.");
				}
				else if(clientSentence.split(":")[0].equals("UPDATE TIME OUT")){
					System.out.println("RECEIVED: " + clientSentence);
				}

				//Request from client to delete file
				else if(clientSentence.split(":")[0].equals("DELETE")){
					System.out.println("RECEIVED: " + clientSentence);
					Process p;
					String command = "rm files/"+ clientSentence.split(":")[1];
					p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
					System.out.println("File deleted succesfully!");
				}
//				//Request from client to REPLICA file
//				else if(clientSentence.split(":")[0].equals("REPLICA")){
//					System.out.println("RECEIVED: " + clientSentence);
//					//FILE IS NOT IN THE SERVER, REQUEST IT
//					String fileName = clientSentence.split(":")[1];
//					File f = new File("files/"+fileName);
//					if(!f.exists()) { 
//						int fileHash = findFileHash(fileName);
//						LinkedList<String> membership = findQuorum(fm.findServerHash(mm.membership), fileHash);
//						for(int i=0;i<membership.size();i++){
//							final int I = i;
//							Thread t =new Thread(new Runnable(){
//								public void run() {
//									try {
//										fm.request(membership.get(I).split("_")[0],localFileName, serverFileName);
//									} catch (IOException e) {
//										// TODO Auto-generated catch block
//										e.printStackTrace();
//									} catch (NoSuchAlgorithmException e) {
//										// TODO Auto-generated catch block
//										e.printStackTrace();
//									}
//								}
//							});	
//							t.start();
//						}
//						
//					}
//				}
				//Request from client to find a file
				else if(clientSentence.split(":")[0].equals("LIST")){
					System.out.println("RECEIVED: " + clientSentence);
					String fileName = clientSentence.split(":")[1];
					fileName = fileName.split("#")[1];
					File f = new File("files/"+fileName);
					//If file exists
					if(f.exists() && !f.isDirectory()) { 
						//Send back ack
						String destination = clientSentence.split(":")[1].split("#")[0].trim();

						Socket socketT = new Socket(destination, serverPort);
						DataOutputStream outToServer = new DataOutputStream(socketT.getOutputStream());
						outToServer.writeBytes(("LIST:"+clientSentence.split(":")[1]+":"+InetAddress.getLocalHost().getHostAddress()) + '\n');
						socketT.close();
						System.out.println("Send back LIST to "+"LIST:"+clientSentence.split(":")[1]+":"+InetAddress.getLocalHost().getHostAddress());
					}
					//File not found
					else{
						String destination = clientSentence.split(":")[1].split("#")[0].trim();

						Socket socketT = new Socket(destination, serverPort);
						DataOutputStream outToServer = new DataOutputStream(socketT.getOutputStream());
						outToServer.writeBytes(("LIST:"+clientSentence.split(":")[1]+"(FILE NOT FOUND):"+InetAddress.getLocalHost().getHostAddress()) + '\n');
						socketT.close();
						System.out.println("Send back LIST NOT FOUND to "+clientSentence.split(":")[1]+":"+InetAddress.getLocalHost().getHostAddress());
					}
				}
			}
			//Handle incoming infor request
			else if(clientSentence.split(":").length==3){
				if(clientSentence.split(":")[0].equals("LIST")){
					System.out.println(clientSentence);
				}
				else if(clientSentence.split(":")[0].equals("HAVE")){
					System.out.println("RECEIVED: " + clientSentence);
					String fileName = clientSentence.split(":")[1];
					String serverfileName = fileName.split("#")[0];
					String localfileName = fileName.split("#")[1];
					String address = clientSentence.split(":")[2];
					if(!localfileName.equals(tempFileName)){
						get(address,localfileName,serverfileName);
						tempFileName = localfileName;
					}
				}
			}


		}
	}

	/**
	 * Function to send download request to servers
	 * @param localPath
	 * @param serverPath
	 * @throws SocketException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException 
	 * @throws SftpException
	 * @throws JSchException
	 */
	public void request(String address, String localFileName, String serverFileName) throws SocketException, IOException, NoSuchAlgorithmException{
		Socket socket = new Socket(InetAddress.getByName(address), serverPort);
		OutputStream os = socket.getOutputStream();
		byte[] contents;
		contents = ("REQUEST:"+LOCAL_ADDRESS+"#"+serverFileName+"#"+localFileName).getBytes();
		os.write(contents);
		os.flush(); 
		socket.close();

	}

	public void get(String address, String localFileName, String serverFileName)throws SocketException, IOException, NoSuchAlgorithmException{

		Socket socket = new Socket(InetAddress.getByName(address), serverPort);

		OutputStream os = socket.getOutputStream();

		byte[] contents;

		contents = ("GET:"+serverFileName+"#"+localFileName).getBytes();
		os.write(contents);
		os.flush(); 
		System.out.println("Request get sent succesfully!");
		//File transfer done. Close the socket connection!
		socket.close();
	}


	/**
	 * Function for replica
	 * @param list
	 * @param fileName
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws NoSuchAlgorithmException 
	 */
	public void replica(String id) throws IOException, InterruptedException, NoSuchAlgorithmException{
		//Send LS
		
		
		
		
		//1. Receive failure(From disseminate)

		//2. Check all files 
		ArrayList<String> fileList = new ArrayList<String>();
		Process p;
		p = Runtime.getRuntime().exec("ls ./files/");
		p.waitFor();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		//Get all file names
		String line = "";
		while ((line = reader.readLine()) != null) {
			fileList.add(line);
//
//			System.out.println(line);
		}
		
		int selfHash = findFileHash(mm.id);
		int failHash = findFileHash(id);
		
		for(String file: fileList){
			
//			int fileHash = findFileHash(file);
//			//Got 3 quorum
//			LinkedList<String> membership = findQuorum(findServerHash(mm.membership), fileHash);
//			for(int i=0;i<membership.size();i++){
//				Socket socket = new Socket(InetAddress.getByName(membership.get(i).split("_")[0]), serverPort);
//				OutputStream os = socket.getOutputStream();
//				byte[] contents;
//				contents = ("REPLICA:"+LOCAL_ADDRESS+"#"+file).getBytes();
//				os.write(contents);
//				os.flush(); 
//				socket.close();
//			}
			
			
			//The crashed one is on the right
			int fileHash = findFileHash(file);
			System.out.println("FILEHASH: "+fileHash);
			System.out.println("SELFHASH: "+selfHash);
			System.out.println("FAILHASH: "+failHash);
			LinkedList<Pair> servers = findServerHash(mm.membership);
			//LinkedList<String> servers = findQuorum(findServerHash(mm.membership), fileHash);
			//replication condition
			int index = 0;
			for(int i = 0; i < servers.size(); i++){
				if(selfHash == servers.get(i).number){
					index = i;
					break;
				}
			}
			//right
			if(fileHash == selfHash && failHash > selfHash){
				if(index == servers.size() - 1){
					put(servers.get(0).id.split("_")[0], file, file);
					System.out.println("Replica file: "+file+" to :"+servers.get(0).id.split("_")[0]);
				}else if(failHash < servers.get(index + 1).number){
					put(servers.get(index + 1).id.split("_")[0], file, file);
					System.out.println("Replica file: "+file+" to :"+servers.get(index + 1).id.split("_")[0]);
				}
				
			}
			//left
			if(fileHash == selfHash && failHash < selfHash){
				if(index == 0){
					put(servers.get(servers.size() - 1).id.split("_")[0], file, file);
					System.out.println("Replica file: "+file+" to :"+servers.get(servers.size() - 1).id.split("_")[0]);
				}else if(failHash > servers.get(index - 1).number){
					put(servers.get(index - 1).id.split("_")[0], file, file);
					System.out.println("Replica file: "+file+" to :"+servers.get(index - 1).id.split("_")[0]);
				}
				
			}
			//left left
			if(fileHash == failHash && failHash < selfHash){
				if(index == 0){
					put(servers.get(index + 1).id.split("_")[0], file, file);
					System.out.println("Replica file: "+file+" to :"+servers.get(index + 1).id.split("_")[0]);
				}else if(failHash > servers.get(index - 1).number){
					if(index == servers.size() - 1){
						put(servers.get(0).id.split("_")[0], file, file);
						System.out.println("Replica file: "+file+" to :"+servers.get(0).id.split("_")[0]);
					}else if(failHash < servers.get(index + 1).number){
						put(servers.get(index + 1).id.split("_")[0], file, file);
						System.out.println("Replica file: "+file+" to :"+servers.get(index + 1).id.split("_")[0]);
					}
				}
			}

		}
		
		
	}


	/**
	 * Function to list all files in current director
	 * @throws IOException 
	 * @throws UnknownHostException 
	 * @throws NoSuchAlgorithmException 
	 * @throws JSchException
	 * @throws SftpException
	 */
	public void list(String sendIP, String fileName, String receiverIP) throws UnknownHostException, IOException, NoSuchAlgorithmException  {
		//FOR EACH ADDRESS, SEND LIST REQUEST TO CHECK IF THE FILE IS THERE
		Socket socket = new Socket(InetAddress.getByName(sendIP), serverPort);
		OutputStream os = socket.getOutputStream();
		byte[] contents;
		contents = ("LIST:"+receiverIP+"#"+fileName).getBytes();
		os.write(contents);
		os.flush(); 
		socket.close();
	}

	/**
	 * Function to delete a file on server side
	 * @param serverFile
	 * @throws IOException 
	 * @throws UnknownHostException 
	 * @throws SftpException 
	 */
	public void delete(String address,String serverFile) throws UnknownHostException, IOException  {
		Socket socket = new Socket(InetAddress.getByName(address), serverPort);

		OutputStream os = socket.getOutputStream();

		byte[] contents;

		contents = ("DELETE:"+serverFile).getBytes();
		os.write(contents);
		os.flush(); 
		//File transfer done. Close the socket connection!
		socket.close();
	}

	/**
	 * Function to split file into pieces
	 * @param f
	 * @throws IOException
	 */
	public void split(String fileName, int NumOfBlock) throws IOException {
		File file = new File(fileName);
		//Number of block
		int blockCounter = 0;

		double fileSize = file.length()/NumOfBlock; 

		byte[] buffer = new byte[(int) Math.ceil(fileSize)];

		System.out.println("file total size is: "+ file.length());
		System.out.println("file size is: "+(int) Math.ceil(fileSize));


		FileInputStream fis = new FileInputStream(file);
		BufferedInputStream bis = new BufferedInputStream(fis);

		int bytesAmount = 0;
		while ((bytesAmount = bis.read(buffer)) > 0) {
			//write each chunk of data into separate file with different number in name
			String filePartName = String.format("%s.%d", file.getName(), blockCounter++);
			File newFile = new File(file.getParent(), filePartName);
			FileOutputStream out = new FileOutputStream(newFile);
			out.write(buffer, 0, bytesAmount);
		}
	}

	public void merge(ArrayList<String> fileNames, String destination) throws IOException{
		FileOutputStream fos = new FileOutputStream(destination);
		BufferedOutputStream mergingStream = new BufferedOutputStream(fos);
		for (String file : fileNames) {
			File f = new File(file);
			Files.copy(f.toPath(), mergingStream);
		}
		mergingStream.close();
	}


	/**
	 * Helper function to find the hash value of the file.
	 * @param filename
	 * @return 
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	public static int findFileHash(String filename) throws NoSuchAlgorithmException, UnsupportedEncodingException
	{
		MessageDigest digest = MessageDigest.getInstance("SHA-1");
		digest.reset();
		digest.update(filename.getBytes("UTF-8"));
		byte[] hash = digest.digest();
		int number = hash[0] & 0xff;//Since there are only 10 servers, we only need 4 bits
		return number;
	}

	/**
	 * Helper function to find hash values for servers
	 * @param members
	 * @param result
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	public LinkedList<Pair> findServerHash(LinkedList<String> members) throws NoSuchAlgorithmException, UnsupportedEncodingException
	{
		LinkedList<Pair> result = new LinkedList<Pair>();
		for(String member: members){
			int number = findFileHash(member);
			result.add(new Pair(member, number));
		}
		Collections.sort(result, (a, b) -> a.number - b.number);

		return result;
	}

	/**
	 * Helper function to get quorum neighbors based on Sever collection and file hash value
	 * @param members
	 * @param hashValue
	 * @return
	 */
	public LinkedList<String> findQuorum(LinkedList<Pair> members, int hashValue){
		LinkedList<String> result = new LinkedList<String>();
		int index = 0;
		for(int i = 0; i < members.size(); i++){
			if(members.get(i).number >= hashValue){
				index = i;
				break;
			}
		}
		//file should be at the neighboring servers?
		int tmp = index == 0 ? members.size() - 1 : index - 1;
		result.add(members.get(tmp).id);
		result.add(members.get(index).id);
		tmp = index == members.size() - 1 ? 0 : index + 1;
		result.add(members.get(tmp).id);
		return result;
	}

	/**
	 * 
	 * @return
	 */
	public MembershipManager getMm() {
		return mm;
	}

	public void setMm(MembershipManager mm) {
		this.mm = mm;
	}

	//Helper class for sorting servers in a virtual ring
	class Pair{
		String id;
		int number;
		public Pair(String id, int number){
			this.id = id;
			this.number = number;
		}
	}
}
