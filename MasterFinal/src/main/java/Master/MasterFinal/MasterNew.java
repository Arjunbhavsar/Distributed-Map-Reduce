package Master.MasterFinal;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MasterNew {

	public static int MASTERPORT;
	private static int PORTNUMBEROFKEYVALUEPAIR;

	public MasterNew(String MasterPort, String KVPPort) {
		MASTERPORT = Integer.parseInt(MasterPort);
		PORTNUMBEROFKEYVALUEPAIR = Integer.parseInt(KVPPort);
	}

	/*
	 * private static void startKeyValueStore(String kvpIP) throws IOException {
	 * //Start Key value store Server Thread thread = new Thread() { public void
	 * run() { Server1.startServer(kvpIP,PORTNUMBEROFKEYVALUEPAIR); } };
	 * thread.start(); }
	 */

	private static void startKeyValueStore(){
		//Start Key value store Server
		Thread thread = new Thread() {
			public void run() {
				//GCPmapreduce.master.Server1.startServer(kvpIP,PORTNUMBEROFKEYVALUEPAIR);
				List<String> instancesList= new ArrayList<String>();
				instancesList.add("test2");

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
	}

/*	
	
	public void startMaster(String masterIP, String book1Name) throws IOException {
		ServerSocket ss = new ServerSocket(MASTERPORT,0, InetAddress.getByName(masterIP));

		try {
			Util utils = new Util();
			String text = utils.readBook();
			List<String> bookParts = utils.divideBook(text);
			System.out.println("Number of Mappers to connect --"+bookParts.size());


			startMapperHandlers(book1Name, ss, bookParts);
			mapperTask(ss, bookParts,book1Name);

			List<List<String>> reducerData =  utils.readDataFromKVp();
			
			System.out.println("Number of Reducers to connect --"+reducerData.size());
			startReducerHandlers(ss,reducerData);
			startReducers(ss);

		}catch (IOException e) {
			System.out.println("Problem With Master Server"+e);
		}
	}

	//Start Mapper Handler
	private void startMapperHandlers(String book1Name, ServerSocket ss, List<String> bookParts) {
		Thread thread = new Thread() {

			public void run() {
				System.out.println("Master ready To Accept Client");
				Socket sock;
				try {

					for(String part :bookParts) {
						sock = ss.accept();
						System.out.println("Mapper Connected");

						DataInputStream input = new DataInputStream(sock.getInputStream());
						DataOutputStream output = new DataOutputStream(sock.getOutputStream());

						//Start Mappers as per the required count
						MapperHandler t= new MapperHandler(sock,book1Name+"#"+part,input,output);
						t.start();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		thread.start();
	}


	private void mapperTask(ServerSocket ss, List<String> bookParts, String book1Name) throws IOException {
		Thread[] threadsM = new Thread[bookParts.size()];

		for (int i = 0; i<bookParts.size();i++) {

			threadsM[i]= new Thread() { 
				public void run() { 
					new Mapper(MASTERPORT,PORTNUMBEROFKEYVALUEPAIR);
				}  
			}; 
			threadsM[i].start();

			try {
				TimeUnit.MILLISECONDS.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		for(int i=0;i<threadsM.length; i++) {
			try {
				threadsM[i].join(20000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("\n-------Task of Mapper Completed---\n");
	}
	private void startReducerHandlers(ServerSocket ss, List<List<String>> reducerData) {

		Thread thread = new Thread() {
			public void run() {
				System.out.println("Master ready To Accept Client");
				Socket sock;
				try {

					for(List<String> part :reducerData) {

						StringBuilder sb = new StringBuilder();
						for(String s:part) {
							sb.append(s).append("\n");
						}

						// TODO Auto-generated method stub
						sock = ss.accept();
						System.out.println("Redeucer Connected");

						DataInputStream input = new DataInputStream(sock.getInputStream());
						DataOutputStream output = new DataOutputStream(sock.getOutputStream());

						//Start Mappers as per the required count
						ReducerHandler rThread= new ReducerHandler(sock,sb.toString(),input,output);
						rThread.start();

					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		thread.start();
	}


	private void startReducers(ServerSocket ss) throws IOException {
		Util utils =new Util();
		List<List<String>> reducers =  utils.readDataFromKVp();

		Thread[] threadsR = new Thread[reducers.size()];

		for(int i= 0; i <reducers.size() ; i++ ) {

			threadsR[i]= new Thread() { 
				public void run() { 
					new Reducer(MASTERPORT,PORTNUMBEROFKEYVALUEPAIR);
				}  
			}; 
			threadsR[i].start();

			try {
				TimeUnit.MILLISECONDS.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		for(int j= 0 ;j<threadsR.length ; j++) {
			try {
				threadsR[j].join(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println("\n-------Task of Reducer Completed---\n");
	}
*/
	public static void main(String[] args) throws IOException {

		File configFile = new File("src/config.properties");

		FileReader reader = new FileReader(configFile);
		Properties props = new Properties();
		props.load(reader);

		String kvpPort = props.getProperty("KVPPort");
		String MasterPort = props.getProperty("MasterPort");
		String masterIP = props.getProperty("IPMaster");
		String kvpIP = props.getProperty("IPMaster");

		MasterNew mNode = new MasterNew(kvpPort,MasterPort);

		String task = props.getProperty("task");
		if("WordCount".equalsIgnoreCase(task)) {
			String book1Path = props.getProperty("book1Path");
			String Book1Name = props.getProperty("Book1Name");

			//Start key value Store
			startKeyValueStore();

			//start Master
		//	mNode.startMaster(masterIP, Book1Name);

			reader.close();
		}
	}
}

