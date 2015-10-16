/*
 * (c) University of Zurich 2014
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
			producer.writeToServer(socket, "PRODUCER");
			// read messages from input file line by line
			// put the client name and colon in front of each message
			// e.g., clientName:....
			// send message until you find ".bye" in the input file
			producer.parseFileToServer(inputFileName, clientName, socket);
		}
		// close connection
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
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
	
	private void writeToServer(Socket socket, String message) {
		try {
			PrintStream writer = new PrintStream(socket.getOutputStream(), true, "UTF-8");
			writer.print(message);
			writer.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	private void parseFileToServer(String fileName, String clientName, Socket socket) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String currentLine;
			while (((currentLine = reader.readLine()) != ".bye") && (currentLine != null)) {
				String producerMessage = clientName + ":" + currentLine;
				writeToServer(socket, producerMessage);
			}
			reader.close();
		}
		catch(IOException ex) {
			ex.printStackTrace();
		}
	}
}

