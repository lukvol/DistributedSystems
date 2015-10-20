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
import java.util.concurrent.LinkedBlockingQueue;

public class Server {
  private static int port; 
  
  // the data structure to store incoming messages, you are also free to implement your own data structure.
  static LinkedBlockingQueue<String> messageStore =  new LinkedBlockingQueue<String>();

  public static void main(String[] args) {
	//port number to listen to
    port = Integer.parseInt(args[0]);
    
    Server server = new Server();
    //starts the server
    server.runServer(port);
  }

  private void runServer(int port) {
	  try {
		//create the server socket which handles the connections
		  ServerSocket serverSocket = new ServerSocket(port);
		  //listen to new connections forever
		  while (true) {
			  //if a client connects to the server, the connection will be accepted
			  Socket connectionToClient = serverSocket.accept();
			  //gets the type of the client
			  String clientType = Server.readLineFromClient(connectionToClient);
			  //starts a new thread for the client
			  Runnable client = new HandleClient(clientType, connectionToClient);
			  Thread clientThread = new Thread(client);
			  clientThread.start();
		  }
	  } catch(IOException ex) {
		  ex.printStackTrace();
	  }
  }
  
  public static String readLineFromClient(Socket socket) {
	  try {
		  //creates an input stream and reads a line from the client
		  InputStreamReader streamReader = new InputStreamReader(socket.getInputStream());
		  BufferedReader reader = new BufferedReader(streamReader);
		  String line = reader.readLine();
		  return line;
	  } catch (IOException ex) {
		  ex.printStackTrace();
		  return null;
	  }   
  }
}

class HandleClient implements Runnable {
	private enum Client {Listener, Producer};
	Client currentClient;
	Socket clientSocket;
	
	public HandleClient(String type, Socket clientSocket) {
		this.clientSocket = clientSocket;
		if (type.equals("LISTENER")) {
			currentClient = Client.Listener;
		} else if (type.equals("PRODUCER")) {
			currentClient = Client.Producer;
		} else {
			return;
		}
	}
	
	public void run () {
		try {
			if (currentClient == Client.Listener) {
				//if client is listener, we print the messages from the producers
				PrintStream writer = new PrintStream(clientSocket.getOutputStream(), true, "UTF-8");
				//like an iterator
				//points to the first element which have to be read
				int pointer = 0;
				//the listener listens forever
				while (true) {
					//if the pointer isn't at the end of the messageStore linkedQueue
					//there are new messages and we need to fetch these
					if (pointer != Server.messageStore.size()) {
						String[] messageStorage = Server.messageStore.toArray(new String[0]);
						for (; pointer < messageStorage.length; pointer++) {
							writer.println(messageStorage[pointer]);
							writer.flush();
						}
					}
				}
			}
			//else is sufficient since we use an enum
			else {
				//creates an input stream to read lines from producers
				InputStreamReader streamReader = new InputStreamReader(clientSocket.getInputStream());
				BufferedReader reader = new BufferedReader(streamReader);
				String currentLine = reader.readLine();
				while(currentLine != null) {
					Server.messageStore.add(currentLine);
					currentLine = reader.readLine();
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
    }
}
