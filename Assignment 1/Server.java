/*
 * (c) University of Zurich 2014
 */

package Assignment1;
import java.net.*;
import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Server {
  private static int port; 
  
  // the data structure to store incoming messages, you are also free to implement your own data structure.
  static LinkedBlockingQueue<String> messageStore =  new LinkedBlockingQueue<String>();

  // Listen for incoming client connections and handle them
  public static void main(String[] args) {
	//port number to listen to
    port = Integer.parseInt(args[0]);
    
    Server server = new Server();
    server.runServer(port);
    
    // this is a blocking operation
    // which means the server listens to connections infinitely
    // when a new request is accepted, spawn a new thread to handle the request
    // keep listening to further requests
    // if you want, you can use the class HandleClient to process client requests
    // the first message for each new client connection is either "PRODUCER" or "LISTENER"

    /*
     * Your implementation goes here
     */
    
  }

  private void runServer(int port) {
	  try {
		// the server listens to incoming connections
		  ServerSocket serverSocket = new ServerSocket(port);
		  
		  while (true) {
			  Socket connectionToClient = serverSocket.accept();
			  String clientType = Server.readLineFromClient(connectionToClient);
			  System.out.println(clientType);
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
		  InputStreamReader streamReader = new InputStreamReader(socket.getInputStream());
		  BufferedReader reader = new BufferedReader(streamReader);
		  String line = reader.readLine();
		  return line;
	  } catch (IOException ex) {
		  ex.printStackTrace();
		  return null;
	  }   
  }
  
  public static void printToClient(Socket socket) {
	  try {
		  PrintStream writer = new PrintStream(socket.getOutputStream(), true, "UTF-8");
		  for (String line : Server.messageStore) {
			  writer.println(line);
		  }
	  } catch (IOException ex) {
		  ex.printStackTrace();
	  }
  }
}

// you can use this class to handle incoming client requests
// you are also free to implement your own class
class HandleClient implements Runnable {
	private enum Client {Listener, Producer};
	Client currentClient;
	Socket clientSocket;
	
	public HandleClient(String type, Socket clientSocket) {
		this.clientSocket = clientSocket;
		if (type.equals("Listener")) {
			currentClient = Client.Listener;
		} else if (type.equals("Producer")) {
			currentClient = Client.Producer;
		} else {
			return;
		}
	}
	
	public void run () {
		try {
			if (currentClient == Client.Listener) {
				if (Server.messageStore.isEmpty()) {
					PrintStream writer = new PrintStream(clientSocket.getOutputStream(), true, "UTF-8");
					writer.println("SERVER: There are no messages stored!");
					writer.flush();
				} else {
					Server.printToClient(clientSocket);
				}
			} else {
				
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
    }
}
