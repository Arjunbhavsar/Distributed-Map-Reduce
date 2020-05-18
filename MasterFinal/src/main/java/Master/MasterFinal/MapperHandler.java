package Master.MasterFinal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class MapperHandler extends Thread{
	private Socket sock;
	private String data;
	final DataInputStream input; 
	final DataOutputStream output; 

	public MapperHandler(Socket sock,String data,DataInputStream input, DataOutputStream output) {
		this.sock = sock;
		this.output = output;
		this.input = input;
		this.data= data;
	}

	public void run() {

		try {

			output.writeUTF(data);
			
			if("Data Recieved By Mapper".equalsIgnoreCase(input.readUTF())) {
				sock.close();	
			}
			
			//String ack = new String();
			/*
			 * if((ack=input.readUTF())!=null) { System.out.println(ack); }
			 */
			/*
			 * output.writeUTF("BOOK1");
			 * 
			 * if("Stored book Name".equalsIgnoreCase(input.readUTF())) {
			 * System.out.println("size of RET  -- " + String.valueOf(ret.size()));
			 * output.flush(); output.writeUTF(String.valueOf(ret.size())); } String ack =
			 * new String(); for(String s : ret) { output.writeUTF(s);
			 * if((ack=input.readUTF())!=null) { System.out.println(ack); } }
			 */
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
