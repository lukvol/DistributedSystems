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

    // the server listens to incoming connections
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

}

// you can use this class to handle incoming client requests
// you are also free to implement your own class
class HandleClient implements Runnable {
	public void run () {        
    }
}
