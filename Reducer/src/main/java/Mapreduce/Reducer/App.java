package Mapreduce.Reducer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Map.Entry;


public class App 
{
	private static Socket sockMaster;
	private static DataInputStream inputMaster;
	private static DataOutputStream outputMaster;
		
	private static Map<String, Integer> map = Collections.synchronizedMap(new TreeMap<String, Integer>());

	private static int PORTNUMBEROFMASTER ; 
	private static String IPADDRESSOFMASTER;

	public App(int masterPort, String masterIP) {
		PORTNUMBEROFMASTER = masterPort;
		IPADDRESSOFMASTER = masterIP;

		try {
			System.out.println("Reducer Started at -----"+InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		StartReducer();
	}

	public void StartReducer() {
		String data = new String() ;
		try {
			sockMaster = new Socket("127.0.0.1", 15000);
			inputMaster  = new DataInputStream(sockMaster.getInputStream());	
			outputMaster = new DataOutputStream(sockMaster.getOutputStream());

			if(null != (data = inputMaster.readUTF())) {
				System.out.println("Size of data ="+data.length());
				String[] lines = data.split("\n");
				for(String line: lines) {
					String[] kvPair = line.split(",");
					if(null != kvPair[0] || null != kvPair[1]) {
						
						String key = kvPair[0];
						//String value = kvPair[1];
						
						String[] keydata = key.split("_");
						String keyExtract = new String();
						if(null != (keyExtract = keydata[1])) {
							if(map.containsKey(keyExtract)) {
								map.put(keyExtract, map.get(keyExtract) + 1);
							}else {
								map.put(keyExtract,1);
							}
						}
					}
				}
				StringBuilder finalCount = new StringBuilder();
				
				for(Entry<String, Integer> entry:map.entrySet()) {
					finalCount.append(entry.getKey()).append(",").append(String.valueOf(entry.getValue())).append("\r\n");
				}
				outputMaster.writeUTF(finalCount.toString());
				
				System.out.println("Reducer task completed at -----"+ InetAddress.getLocalHost().getHostAddress());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main( String[] args )
	{
		File configFile = new File("config.properties");

		FileReader reader = null;
		try {
			reader = new FileReader(configFile);
		} catch (FileNotFoundException e) {
			System.out.println("Configuration File Not Found");
		}

		Properties props = new Properties();
		try {
			props.load(reader);
			int MasterPort = Integer.parseInt(props.getProperty("MasterPort"));
			String masterIP = props.getProperty("IPMaster");

			System.out.println("Master value port+"+MasterPort);
			System.out.println("Master value IP+"+masterIP);

			new App(MasterPort,masterIP);

			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
