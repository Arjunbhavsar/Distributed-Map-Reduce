package Mapreduce.mapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.util.Date;
import java.util.Properties;

public class App 
{
	private static Socket sockMaster;
	private static DataInputStream inputMaster;
	private static DataOutputStream outputMaster;
	private static DataInputStream inputKVP;
	private static DataOutputStream outputKVP;
	
	private static int PORTNUMBEROFMASTER ; 
	private static int PORTNUMBEROFKEYVALUEPAIR;
	
	private static String IPADDRESSOFKVP;
	private static String IPADDRESSOFMASTER;
	
	
	/*
	 * public App(int portNumber, int portNumberOfKeyValuePair) {
	 * this.PORTNUMBEROFMASTER = portNumber; this.PORTNUMBEROFKEYVALUEPAIR =
	 * portNumberOfKeyValuePair;
	 * 
	 * ProcessBuilder pb = new ProcessBuilder("java", "Mapper");
	 * System.out.println("Mapper Process Started"); Process p = null; try { p =
	 * pb.start(); } catch (IOException e) {
	 * System.out.println("problem while starting the process"); } startMapper();
	 * p.destroy(); }
	 */

	public App(int kvpPort, String kvpIP, int masterPort, String masterIP) {
		
		PORTNUMBEROFMASTER = masterPort;
		IPADDRESSOFMASTER =masterIP;
		
		IPADDRESSOFKVP =kvpIP;
		PORTNUMBEROFKEYVALUEPAIR = kvpPort;
		
		System.out.println("Mapper Process Started");
		
		startMapper();
	}

	public static String getdataFromMaster() {
		String data = new String() ;
		try {
			System.out.println("connecting with master At:--"+ IPADDRESSOFMASTER+"--"+ PORTNUMBEROFMASTER);
			
			sockMaster = new Socket(IPADDRESSOFMASTER, PORTNUMBEROFMASTER);
			
			System.out.println("Collecting Data from Master");
			
			inputMaster  = new DataInputStream(sockMaster.getInputStream());	
			outputMaster = new DataOutputStream(sockMaster.getOutputStream());

			if( null != (data =inputMaster.readUTF())){
				outputMaster.writeUTF("Data Recieved By Mapper");
			}
			sockMaster.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return data;
	}

	public static void startMapper() {
		String data = getdataFromMaster();

		Socket sockKVM = null;
		try {
			System.out.println("Mapper Started");

			sockKVM = new Socket(IPADDRESSOFKVP, PORTNUMBEROFKEYVALUEPAIR);
			inputKVP = new DataInputStream(sockKVM.getInputStream());
			outputKVP = new DataOutputStream(sockKVM.getOutputStream());

			String [] dataset = data.split("#");
			String bookName = new String();
			String bookpart = new String();
			if (null != dataset[0] && null != dataset[1]) {
				bookName = dataset[0];
				bookpart = dataset[1];
			}

			String[] readData = bookpart.split("[^a-zA-Z]");

			System.out.println("Number of kVM Stores" +readData.length);
			StringBuilder sb= null;
			String recieveMessege = new String();

			for(String str :readData) {
				if(null != str) {
					if(!str.isEmpty()) {
						Date date= new Date();
						String time = String.valueOf(date.getTime());
						sb = new StringBuilder();
						sb.append("set ").append(bookName + "_").append(str).append("_"+time).append(" 10 ").append("1");
						outputKVP.writeUTF(sb.toString());

						if((recieveMessege=inputKVP.readUTF())!=null) {
							//System.out.println(recievMessagge);	
						}
					}
				}
			}
			// Key Value Store
			sockKVM.close();
			sockMaster.close();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main( String[] args )
	{

		File configFile = new File("src/config.properties");

		FileReader reader = null;
		try {
			reader = new FileReader(configFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			System.out.println("Configuration File Not Found");
		}


		Properties props = new Properties();
		try {
			props.load(reader);
			String kvpIP = props.getProperty("IPKVP");
			int kvpPort = Integer.parseInt(props.getProperty("KVPPort"));
			
			
			int MasterPort = Integer.parseInt(props.getProperty("MasterPort"));
			String masterIP = props.getProperty("IPMaster");
			
			System.out.println("key value port+"+kvpPort);
			System.out.println("key value IP+"+kvpIP);
			
			
			System.out.println("Master value port+"+MasterPort);
			System.out.println("Master value IP+"+masterIP);
			
			new App(kvpPort,kvpIP,MasterPort,masterIP);
			
			reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

}
