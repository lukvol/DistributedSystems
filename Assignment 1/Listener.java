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

public class Listener {
    public static void main(String [] args) {
    	String serverName = args[0];
    	int port = Integer.parseInt(args[1]);
      
    	Listener listener = new Listener();
      
    	// create connection to server
    	Socket socket = listener.connect(serverName, port);
    	if (socket != null) {
    		// send the string  "LISTENER" to server to authenticate
    		listener.writeToServer(socket, "LISTENER");
    		// reads producer messages from server
    		listener.readFromServer(socket);
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
			//create a new output stream to send authentification to server
			PrintStream writer = new PrintStream(socket.getOutputStream(), true, "UTF-8");
			writer.println(message);
			writer.flush();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	private void readFromServer(Socket socket) {
		try {
			//create a new input stream to read from server...
			InputStreamReader streamReader = new InputStreamReader(socket.getInputStream());
			BufferedReader reader = new BufferedReader(streamReader);
			//... until forever
			while(true) {
				String currentLine;
				while((currentLine = reader.readLine()) != null) {
					System.out.println(currentLine);
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}