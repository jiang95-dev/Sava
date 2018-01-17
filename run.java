import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Scanner;

public class Run {
	private static FileManager fm;
	private static MembershipManager mm;
	
	public static void main(String args[]) throws IOException, NoSuchAlgorithmException {
		//MembershipManager
		mm = new MembershipManager();
		//File manager object
		fm = new FileManager();
		
		fm.setMm(mm);
		mm.setFm(fm);
		
		//Scanner to read user input 
		Scanner in = new Scanner(System.in);

		while(true){

			if(in.hasNextLine()){
				String userInput = in.nextLine();
				//Help command
				if(userInput.contains("help")){
					System.out.println("To upload a file, e.g: put localPath ServerPath");
					System.out.println("To download a file, e.g: get localPath ServerPath");
					System.out.println("To delete a file, e.g: delete path");
					System.out.println("To list files, e.g: ls or ls path");
					System.out.println("To leave, e.g: exit");
				}
				else if(userInput.contains("join")){
					mm.join();
					mm.run();
				}
				else if(userInput.contains("leave")){
					mm.leave();
				}else if(userInput.contains("list")){
					mm.printList();
				}
				//**************************
				//**  PUT  *****
				//**************************
				//Upload file to server from local machine to server machine
				else if(userInput.contains("put")){
					//Parameters length checking
					if(userInput.split(" ").length == 3){
						String localFileName = userInput.split(" ")[1];
						String serverFileName = userInput.split(" ")[2];

						//Parameters: localFileName, serverFileName
						//eg: put test.txt serverSideFileName


						// FIGURE OUT WHERE TO PUT THE FILE BASED ON MEMBERSHIP LIST ROUTING
						int fileHash = fm.findFileHash(serverFileName);
						//Got 3 quorum
						LinkedList<String> membership = fm.findQuorum(fm.findServerHash(mm.membership), fileHash);
						for(int i=0;i<membership.size();i++){
							final int I = i;
							Thread t =new Thread(new Runnable(){
								public void run() {
									try {
										fm.put(membership.get(I).split("_")[0], localFileName, serverFileName);
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							});	
							t.start();
						}

					}
					else{
						System.out.println("Missing parameters! e.g: put localFileName serverFileName");
					}

				}
				//**************************
				//**  GET  *****
				//**************************
				//Download file from server to local machine
				else if(userInput.contains("get")){
					//Parameters length checking
					if(userInput.split(" ").length == 3){
						String serverFileName = userInput.split(" ")[1];
						String localFileName = userInput.split(" ")[2];

						int fileHash = fm.findFileHash(serverFileName);
						LinkedList<String> membership = fm.findQuorum(fm.findServerHash(mm.membership), fileHash);
						for(int i=0;i<membership.size();i++){
							final int I = i;
							Thread t =new Thread(new Runnable(){
								public void run() {
									try {
										fm.request(membership.get(I).split("_")[0],localFileName, serverFileName);
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (NoSuchAlgorithmException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							});	
							t.start();
						}
						//Parameters: localPath, serverPath
						//eg: get test1.txt test.txt

					}
					else{
						System.out.println("Missing parameters! e.g: get localPath ServerPath");
					}

				}
				//**************************
				//**  DELETE  *****
				//**************************
				//Delete a file on server side
				else if(userInput.contains("delete")){
					//Parameters length checking
					if(userInput.split(" ").length == 2){
						String serverFileName = userInput.split(" ")[1];

						int fileHash = fm.findFileHash(serverFileName);
						//Got 3 quorum
						LinkedList<String> membership = mm.membership;
						for(int i=0;i<membership.size();i++){
							fm.delete(membership.get(i).split("_")[0],serverFileName);
						}
					}
					else{
						System.out.println("Missing parameters! e.g: delete path");
					}
				}
				//List all files from server
				else if(userInput.contains("ls")){
					//Parameters length checking
					if(userInput.split(" ").length == 2){
						String fileName = userInput.split(" ")[1];
						int fileHash = fm.findFileHash(fileName);
						//Got 3 quorum
						LinkedList<String> membership = fm.findQuorum(fm.findServerHash(mm.membership), fileHash);
						for(int i=0;i<membership.size();i++){
							fm.list(membership.get(i).split("_")[0],fileName,InetAddress.getLocalHost().getHostAddress());
						}

					}
					else{
						System.out.println("Missing parameters! e.g: ls fileName");
					}
				}
				else if(userInput.contains("store")){
					StringBuffer output = new StringBuffer();
					Process p;
					try {
						p = Runtime.getRuntime().exec("ls ./files/");
						p.waitFor();
						BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

						String line = "";
						while ((line = reader.readLine()) != null) {
							output.append(line + "\n");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println(output.toString());
				}
				//**************************
				//**  TEST for hashing*****
				//**************************
				else if(userInput.contains("test")){
					//try {
					////						byte[] asd = fm.encrypt(userInput);
					////						for(int i=0;i<asd.length;i++){
					////							System.out.printf("0x%02X", asd[i]);
					////						}
					//
					//					} catch (NoSuchAlgorithmException e) {
					//						e.printStackTrace();
					//					}
					//					//					fm.exit();
					//					//					break;
				}
				else{
					System.out.println("Illegal command, please type again!");
					System.out.println("Type \"help\" to get command list");
				}

			}
		}
	}

}
