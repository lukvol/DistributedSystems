/*
 * (c) University of Zurich 2014
 */


package Assignment1;

import java.net.*;
import java.io.*;

public class Listener
{
    public static void main(String [] args)
   {
      String serverName = args[0];
      int port = Integer.parseInt(args[1]);
      
      Listener listener = new Listener();
      
	  // create connection to server
      Socket socket = listener.connect(serverName, port);
      if (socket != null) {
    	  // send the string  "LISTENER" to server first!!
    	  listener.writeToServer(socket, "Listener");
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
			PrintStream writer = new PrintStream(socket.getOutputStream(), true, "UTF-8");
			writer.println(message);
			writer.flush();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	private void readFromServer(Socket socket) {
		try {
			  // continuously receive messages from server
		      // do not close the connection- keep listening to further messages from the server.
			InputStreamReader streamReader = new InputStreamReader(socket.getInputStream());
			BufferedReader reader = new BufferedReader(streamReader);
			while(true) {
				String currentLine;
				while((currentLine = reader.readLine()) != null) {
				      // using stdout to print out messages Received
					System.out.println(currentLine);
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
