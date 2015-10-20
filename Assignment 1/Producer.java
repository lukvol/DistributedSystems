/*
 * (c) University of Zurich 2014
 * 
 * Author:
 * Lukas Vollenweider (13-751-888)
 * 
 */

package Assignment1;

import java.net.*;
import java.io.*;

public class Producer {
	public static void main(String[] args) {
		String serverName = args[0];
		int port = Integer.parseInt(args[1]);
		String clientName = args[2];
		String inputFileName = args[3];
		
		Producer producer = new Producer();
		
		// create connection to server
		Socket socket = producer.connect(serverName, port);
		if (socket != null) {
			// send the string "PRODUCER" to server first
			producer.writeLineToServer(socket, "PRODUCER");
			//parse file and write it line by line to server
			producer.parseFileToServer(inputFileName, clientName, socket);
		}
	}
	
	private Socket connect(String serverName, int port) {
		try {
			//connect to server
			return new Socket(serverName, port);
		} catch(IOException ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	private void writeLineToServer(Socket socket, String message) {
		try {
			//create output stream to write to server
			PrintStream writer = new PrintStream(socket.getOutputStream(), true, "UTF-8");
			writer.println(message);
			writer.flush();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	private void parseFileToServer(String fileName, String clientName, Socket socket) {
		try {
			//load file and read it line by line
			//if there is a line and the line is not ".bye", than send it directly to server
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String currentLine = reader.readLine();
			while ((!(currentLine.equals(".bye")) && (currentLine != null))) {
				String producerMessage = clientName + ":" + currentLine;
				writeLineToServer(socket, producerMessage);
				currentLine = reader.readLine();
			}
			//close connection: if a stream get closed, the socket get closed to so there is no need
			//to close the socket separately
			reader.close();
		}
		catch(IOException ex) {
			ex.printStackTrace();
		}
	}
}