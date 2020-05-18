package Master.MasterFinal;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Util {

	//parse the book and return the string 
	public String readBook() {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("HeartofDarkness.txt"));
		} catch (FileNotFoundException e) {
			System.out.println("Problem While reading data from book" + e);
		}
		StringBuilder sb = new StringBuilder();
		try {
			String data;
			while(null != (data=br.readLine())) {
				sb.append(data);
			}
		} catch (IOException e) {
			System.out.println(e);
		}
		return sb.toString() ;
	}

	@SuppressWarnings("resource")
	public List<List<String>> readDataFromKVp(String kvpIP, int kvpPort) throws IOException {
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("data.csv"));
		} catch (FileNotFoundException e) {
			System.out.println("Problem While reading data from KVP" + e);
		}

		List<String> totalString = new ArrayList<String>();

		String data;
		while(null !=(data = br.readLine())) {
			totalString.add(data);
		}

		final AtomicInteger counter = new AtomicInteger();

		Collection<List<String>> result = totalString.stream()
				.collect(Collectors.groupingBy(it -> counter.getAndIncrement() / 2000)).values();

		List<List<String>> finalList = new ArrayList<>(result);
		return finalList;
		/*
		System.out.println("kvpIP=="+kvpIP);
		System.out.println("kvpPort=="+kvpPort);

		Socket sock = new Socket(InetAddress.getByName("10.128.0.13"), 15010);

		DataOutputStream output = new DataOutputStream(sock.getOutputStream());
		DataInputStream input = new DataInputStream(sock.getInputStream());

		int count = 0;
		output.writeUTF("getall");
		if((count=input.readInt())!=0) {
			System.out.println(count);	
		}
		StringBuilder totalString1 = new StringBuilder();

		String ack = new String();
		for (int i= 0; i<count;i++) {

			if((ack=input.readUTF())!=null) {
				totalString1.append(ack);	
			}
		}

		System.out.println(totalString1.length());

		List<String> totalString = new ArrayList<String>();

		if(null != totalString1 && !"".equalsIgnoreCase(totalString1.toString())) {
			totalString.addAll(Arrays.asList(totalString1.toString().split("\\n")));
		}
		 
		final AtomicInteger counter = new AtomicInteger();

		Collection<List<String>> result = totalString.stream()
				.collect(Collectors.groupingBy(it -> counter.getAndIncrement() / 2000)).values();

		List<List<String>> finalList = new ArrayList<>(result);
		return finalList;
		*/
	}


	public List<String> divideBook(String book) {
		int size=40000;
		//int size = book.length()/maxSize;

		List<String> ret = new ArrayList<String>((book.length() + size - 1) / size);

		for (int start = 0; start < book.length(); start += size) {
			ret.add(book.substring(start, Math.min(book.length(), start + size)));
		}
		return ret;
	}

}
