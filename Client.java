import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*	
 *  The client must be the 01 machine for now.
 *	Special string indicating end of output to avoid blocking.
 * 	Using socket array and thread pool.
 */

public class Client {
	
	private int numServer = 9;
	private static String command;
	private Socket[] sockets;
	private DataInputStream[] in;
	private DataOutputStream[] out;
	private boolean[] isAlive; //indicate which socket is still alive
	private FileWriter clientFW; //local filewriter
	private int[] lines; //collecting line counts
	
	/*
	 *  Whenever the user put in a command, read it, grep the local machine
	 *  and then grep the remote servers.
	 */
	public static void main(String[] args){
		Client client = new Client();
		Scanner in = new Scanner(System.in);
		while(true){
			if(in.hasNextLine()){
				command = in.nextLine();
				client.clientGrep();
				client.run();
			}
			
		}
	}
	
	/*
	 *  The constructor is to initialize the socket array and connect to each server.
	 *  Only the server number will be passed to threads.
	 */
	public Client(){
		lines = new int[numServer + 1];
		sockets = new Socket[numServer];
		in = new DataInputStream[numServer];
		out = new DataOutputStream[numServer];
		isAlive = new boolean[numServer];
		try {
			clientFW = new FileWriter("output1.txt", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(int i = 0; i < numServer; i++){
			String address = "172.22.146." + String.valueOf(153 + i);
			try {
				sockets[i] = new Socket(address, 4233);
				in[i] = new DataInputStream(sockets[i].getInputStream());
				out[i] = new DataOutputStream(sockets[i].getOutputStream());
				isAlive[i] = true;
				System.out.println("connected to server " + String.valueOf(i + 2));
			} catch (IOException e) {
				System.out.println("server " + (i + 2) + " is not on");
			}
		}
		
	}
	
	/*
	 *  This method is to create thread pool, start threads, join threads
	 *  and count the total number of lines.
	 */
	public void run(){
		Thread[] threads = new Thread[numServer];
		//create threads
		for(int i = 0; i < numServer; i++){
			if(!isAlive[i])
				continue;
			final int serverId = i;
			threads[i] = new Thread(new Runnable(){
				public void run(){
					run_thread(serverId);
				}
			});
		}
		//run threads
		for(int i = 0; i < numServer; i++){
			if(!isAlive[i])
				continue;
			threads[i].start();
		}
		for(int i = 0; i < numServer; i++){
			if(!isAlive[i])
				continue;
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//count sum
		int sum = 0;
		for(int count : lines)
			sum += count;
		System.out.println("total count: " + sum);
		lines = new int[numServer + 1];
	}
	
	/*
	 *  This is the task for threads. Thread is going to send the command to the
	 *  server, read the result and write it to the local file.
	 */
	public void run_thread(int id){
		try {
			String result = null;
			
			FileWriter fw = new FileWriter("output" + (id + 2) + ".txt", true);
			BufferedWriter bw = new BufferedWriter(fw);
		    PrintWriter pw = new PrintWriter(bw);

			out[id].writeUTF(command);
			pw.println(command);
			int line = 0;
			while(true){//non-blocking
				result = in[id].readUTF();
				if(result.equals("\n"))
					break;
				line++;
				pw.println(result);
			}
			lines[id + 1] = line;
			System.out.println("id:" + (id + 2) + " " + "line count: " + line);
			pw.println("line count: " + line);
			
			pw.close();
		} catch (IOException e) {
			System.out.println("server " + (id + 2) + " is down");
			isAlive[id] = false;
		}
	}
	
	/*
	 *  There's also log file in the local machine. So this method is doing grep
	 *  on the local machine. This method is similar as the one in the server.
	 */
	public void clientGrep(){
		Process p;
		try {
			p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
			System.out.println("started to grep");
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			int count = 0;
			clientFW.write(command + "\n");
			while ((line = reader.readLine()) != null) {
				clientFW.write(line + "\n");
				count++;
			}
			clientFW.write("line count: " + count + "\n");
			lines[0] = count;
			System.out.println("id:1 line count: " + count);
			clientFW.flush();
			reader.close();
		} catch (IOException e) {
			System.out.println("error caused by clientGrep");
		}
	}
}
