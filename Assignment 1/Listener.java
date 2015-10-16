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
      DataInputStream streamIn ;
      DataOutputStream streamOut;
      
	  // create connection to server
	  // send the string  "LISTENER" to server first!!
	  // continuously receive messages from server
      // using stdout to print out messages Received
      // do not close the connection- keep listening to further messages from the server.
   }
}
