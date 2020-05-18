package Master.MasterFinal;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class ReducerHandler extends Thread{
	private Socket sock;
	private String data;
	final DataInputStream input; 
	final DataOutputStream output;
	private static Map<String, Integer> map = Collections.synchronizedMap(new TreeMap<String, Integer>());


	public ReducerHandler(Socket sock,String data, DataInputStream input, DataOutputStream output) {
		this.sock = sock;
		this.output = output;
		this.input = input;
		this.data = data;
	}	

	void setMap()  {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("OutputWordCount.csv"));
		} catch (FileNotFoundException e1) {
			System.out.println(e1);
		}
		String data= new String();
		try {
			while(null != (data=br.readLine())) {
				String[] readData = data.split(",");
				if (null != readData[0] && null != readData[1]) {
					ReducerHandler.map.put(readData[0], Integer.parseInt(readData[1]));
				}
			}
		} catch (IOException e) {
			System.out.println(e);
		}
	}
	
	public void run() {

		setMap();
		try { 
			System.out.println("Data Size"+ data.substring(0,100));
			output.writeUTF(data);

			try {
				TimeUnit.MILLISECONDS.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			String data = new String();
			if(null != (data = input.readUTF())) {
				String[] counts  = data.split("\r\n");
				for(String entry :counts) {
					String[] keyData = entry.split(",");
					if(null != keyData[0]  && null != keyData[1]) {
						String key =keyData[0];
						Integer value = Integer.parseInt(keyData[1]);
						if(map.containsKey(key)) {
							map.put(key, map.get(key) + value);
						}else {
							map.put(key,value);
						}
					}
				}
			}else {
				System.out.println("No data received");
			}

		} catch (IOException e) { 
			// TODO	  Auto-generated catch block e.printStackTrace(); 
		}

		System.out.println("Data Size"+ map.size());
		StringBuilder wordcountSB = new StringBuilder(); 
		
		for(Entry<String, Integer> entry:map.entrySet()) {
			wordcountSB.append(entry.getKey()).append(",").append(String.valueOf(entry.getValue())).append("\r\n");
		}
		
		
	
		writeToFile(wordcountSB.toString());
		
		
		
		/*
		 * try(Writer write = new FileWriter("OutputWordCount.csv")){ for(Entry<String,
		 * Integer> entry:map.entrySet()) {
		 * write.append(entry.getKey()).append(",").append(String.valueOf(entry.getValue
		 * ())).append("\r\n"); } }catch(IOException e) {
		 * System.out.println("File Handling Issue: "+ e); }
		 */
		
	}
	
	private void writeToFile(String text) {
		try {
			FileWriter fw = new FileWriter("wordCount.txt", true);
			fw.write(text);
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
