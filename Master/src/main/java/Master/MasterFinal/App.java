package Master.MasterFinal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;

public class App {

	public static int MASTERPORT;
	private static int PORTNUMBEROFKEYVALUEPAIR;

	private static String IPADDRESSOFKVP;
	private static String IPADDRESSOFMASTER;

	private Queue<Socket> mapperInstanceQueue = new LinkedList<>();
	private Queue<Socket> reducerInstanceQueue = new LinkedList<>();
	List<String> mapperInstances = new ArrayList<String>(Arrays.asList("mapper-1","mapper-2","mapper-3"));
	List<String> reducerInstances = new ArrayList<String>(Arrays.asList("reducer-1","reducer-3","reducer-4"));
	public App(String MasterPort, String KVPPort, String kvpIP) {
		MASTERPORT = Integer.parseInt(MasterPort);
		PORTNUMBEROFKEYVALUEPAIR = Integer.parseInt(KVPPort);
		IPADDRESSOFKVP = kvpIP;

	}

	/*
	 * private static void startKeyValueStore(String kvpIP) throws IOException {
	 * //Start Key value store Server Thread thread = new Thread() { public void
	 * run() { Server1.startServer(kvpIP,PORTNUMBEROFKEYVALUEPAIR); } };
	 * thread.start(); }
	 */

	private static void startKeyValueStore() {
		//Start Key value store Server
		Thread thread = new Thread() {
			public void run() {
				//GCPmapreduce.master.Server1.startServer(kvpIP,PORTNUMBEROFKEYVALUEPAIR);
				List<String> instancesList= new ArrayList<String>();
				instancesList.add("kvs");

				try {
					GcloudInstanceStart.startInstances(instancesList, "arjun-bhavsar", "us-central1-a");
					//testStop.stopInstances(instancesList, "arjun-bhavsar", "us-central1-a");
				} catch (IOException | GeneralSecurityException e) {
					// TODO Auto-generated catch block
					System.out.println("Exception-"+e);
				}
			}
		};
		thread.start();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			System.out.println("Time Interrruption");
		}
	}

	public void startMaster(String book1Name) throws IOException {

		InetAddress IP = null;
		try {
			IP = InetAddress.getLocalHost();
		} catch (UnknownHostException e2) {
			System.out.println("Problem while fetching ip Address:"+ e2);
		}
		System.out.println("IP of my system is := "+IP);
		System.out.println("port of the master is= "+ MASTERPORT);
		//ServerSocket ss = new ServerSocket(MASTERPORT,0, InetAddress.getByName("127.0.0.1"));
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(MASTERPORT,50, IP);
		} catch (IOException e1) {
			System.out.println("Problem With Server Start"+e1);
		}

		Util utils = new Util();
		String text = utils.readBook();
		List<String> bookParts = utils.divideBook(text);
		
		if(bookParts.size()>=3) {
			bookParts = bookParts.subList(0, 2);
		}
		System.out.println("Number of Mappers to connect --"+bookParts.size());


		mapperTask(ss, bookParts,book1Name);
		
			try {
				startMapperHandlers(book1Name, ss, bookParts);
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("Problem Starting The Handler.");
			}
		 
		List<List<String>> reducerData =  utils.readDataFromKVp(IPADDRESSOFKVP,PORTNUMBEROFKEYVALUEPAIR);

		if(reducerData.size()>=3) {
			reducerData = reducerData.subList(0, 3);
		}
		System.out.println("Number of Reducers to connect --"+reducerData.size());

		startReducers(ss,reducerData.size());

		try {
			startReducerHandlers(ss,reducerData);
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		stopVMs(mapperInstances);
		stopVMs(reducerInstances);

	}

	//Start Mapper Handler
	private void startMapperHandlers(String book1Name, ServerSocket ss, List<String> bookParts) throws IOException, InterruptedException {

		Socket socketMapper;

		if(null != bookParts && !bookParts.isEmpty()) {

			if(bookParts.size()>=3) {
				for (int i = 0; i < 3; i++) {
					try {
						socketMapper = ss.accept();
						mapperInstanceQueue.add(socketMapper);
						System.out.println("master and mapper Communication Established at:-"+socketMapper);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}else {
				for (int i = 0; i < bookParts.size(); i++) {
					try {
						socketMapper = ss.accept();
						mapperInstanceQueue.add(socketMapper);
						System.out.println("master and mapper Communication Established at:-"+socketMapper);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			Socket sock;		
			for(String part :bookParts) {
				sock = mapperInstanceQueue.poll();
				System.out.println("Mapper Connected");

				DataInputStream input = new DataInputStream(sock.getInputStream());
				DataOutputStream output = new DataOutputStream(sock.getOutputStream());

				//Start Mappers as per the required count
				MapperHandler t= new MapperHandler(sock,book1Name+"#"+part,input,output);
				t.start();

				Thread.sleep(5000);
				mapperInstanceQueue.add(sock);
			}
		}
	}


	private void startVMs(List<String> instancesList) {		
		Thread thread = new Thread() {
			public void run() {
				try {
					GcloudInstanceStart.startInstances(instancesList, "arjun-bhavsar", "us-central1-a");
				} catch (IOException | GeneralSecurityException e) {
					System.out.println("Exception-"+e);
				}
			}
		};
		thread.start();
	} 

	private void stopVMs(List<String> instancesList) {		
		Thread thread = new Thread() {
			public void run() {
				try {
					GcloudInstanceStop.stopInstances(instancesList, "arjun-bhavsar", "us-central1-a");
				} catch (IOException | GeneralSecurityException e) {
					System.out.println("Exception-"+e);
				}
			}
		};
		thread.start();
	} 

	private void mapperTask(ServerSocket ss, List<String> bookParts, String book1Name) throws IOException {

		if(null != bookParts && !bookParts.isEmpty()) {
			if(bookParts.size()>=3) {
				startVMs(mapperInstances);
			}else {
				startVMs(mapperInstances.subList(0, bookParts.size()));
			}
		}
		System.out.println("\n-------All reducers Launched---\n");
	}
	private void startReducerHandlers(ServerSocket ss, List<List<String>> reducerData) throws InterruptedException, IOException {

		Socket socketReducer;

		if(null != reducerData && !reducerData.isEmpty()) {

			if(reducerData.size()>=3) {
				for (int i = 0; i < 3; i++) {
					try {
						socketReducer = ss.accept();
						reducerInstanceQueue.add(socketReducer);
						System.out.println("Reducer and mapper Communication Established at:-"+socketReducer);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}else {
				for (int i = 0; i < reducerData.size(); i++) {
					try {
						socketReducer = ss.accept();
						reducerInstanceQueue.add(socketReducer);
						System.out.println("Reducer and mapper Communication Established at:-"+socketReducer);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			Socket sock;		
			for(List<String> part :reducerData) {

				StringBuilder sb = new StringBuilder();
				for(String s:part) {
					sb.append(s).append("\n");
				}

				sock = reducerInstanceQueue.poll();
				System.out.println("Reducer Connected");

				DataInputStream input = new DataInputStream(sock.getInputStream());
				DataOutputStream output = new DataOutputStream(sock.getOutputStream());

				//Start reducers as per the required count
				ReducerHandler rThread= new ReducerHandler(sock,sb.toString(),input,output);
				rThread.start();

				Thread.sleep(5000);
				reducerInstanceQueue.add(sock);
			}
		}
	}

	private void startReducers(ServerSocket ss, int reducers) throws IOException {
		Util utils =new Util();
		//List<List<String>> reducers =  utils.readDataFromKVp(IPADDRESSOFKVP,PORTNUMBEROFKEYVALUEPAIR);

		if(reducers!= 0) {
			if(reducers>=3) {
				startVMs(reducerInstances);
			}else {
				startVMs(reducerInstances.subList(0, reducers));
			}
		}
		System.out.println("\n-------All reducers Launched---\n");
	}


	public static void main(String[] args) throws IOException {

		File configFile = new File("config.properties");

		FileReader reader = new FileReader(configFile);
		Properties props = new Properties();
		props.load(reader);

		String kvpPort = props.getProperty("KVPPort");
		String kvpIP = props.getProperty("IPKVP");

		String MasterPort = props.getProperty("MasterPort");
		String masterIp = props.getProperty("IPMaster");

		App mNode = new App(MasterPort,kvpPort,kvpIP);


		System.out.println("kvPort"+ kvpPort);
		System.out.println("kvPort"+ kvpPort);

		System.out.println("master Port"+ MasterPort);
		System.out.println("master IP"+ masterIp);

		PrintWriter file = null;
		try {
			file = new PrintWriter("wordCount.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		file.print("");
		file.close();

		String task = props.getProperty("task");
		if("WordCount".equalsIgnoreCase(task)) {
			String Book1Name = props.getProperty("Book1Name");

			//Start key value Store
			startKeyValueStore();

			//start Master
			mNode.startMaster(Book1Name);
			reader.close();
		}
	}
}
