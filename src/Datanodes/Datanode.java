/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Datanodes;

import java.io.BufferedReader;
import java.io.*;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * This class is an implementation of the Data node storing data in the system.
 * @author Ibrahim-Abdullah
 * @since 29-4-2017
 * @version version 1.0
 * 
 * 
 */
public class Datanode implements Runnable
{
    //The IP address of the namenode
    String namenode_address;
    
    //The port on which the namenode is always listening on
    int namenode_portNumber;
    
    //The id of the datanode
    int datanodeId;
   //Datanode portnumber for listening for other request
    int datanode_portNumber;
    ServerSocket serverSocket;
    
    
    //A hashmap to store data keys and values
    //The String is the key if each data values stored in an arraylist.
    HashMap<String,String> nodeData = new HashMap<String,String>();
    //static ArrayList<Integer> datanodesPortNumbers = new ArrayList<>();

    static File logFile = new File("LogFile.txt");
    /**
     * The constructor create an instance of the heartbeat class and use thread
     * to run its run method which send heartbeat info to the name node.
     * @param node_id The id of this data node
     * @param namenode_address The IP address of the name node to send heart beat 
     * messages to by the data node.
     * @param namenode_portNumber The port number on which the name node is listening
     * for heart beat messages from data nodes.
     */
    public Datanode(int node_id,String namenode_address,int namenode_portNumber)
    {
        this.serverSocket = null;
        this.datanodeId = node_id;
        this.namenode_address = namenode_address;
        this.namenode_portNumber = namenode_portNumber;
        this.loadData(datanodeId);
        
    }
    
    
    /**
     * This is the main method of the Data node1 class. 
     * This method receive the data node id, the address of the name node and the port number
     * on which the data node is listening for heartbeat messages.
     * 
     * It create a thread that is used the run the run method of the data node class.
     * @param args Command line argument
     */
    public static void main(String args[])
    {
        int datanodeID;
        String namenodeAddress;
        int namenodePortnumber;
        if (args.length < 3) 
        {
            System.out.println("Error: Please enter datanode id, namenode address and portnumber");
            System.exit(1);
        } else {
            datanodeID= Integer.valueOf(args[0]).intValue();
            namenodeAddress = args[1];
            namenodePortnumber = Integer.valueOf(args[2]).intValue();
            
            //Load data to be stored at this datanode from the file
            //datanode.loadData(datanodeID);
            //Create an instance of the heartbeat class 
            //This will send constant messages to the namenode to show it sill alive
            Datanode node = new Datanode(datanodeID,namenodeAddress,namenodePortnumber);
            Thread t1 = new Thread(node);
            SendHeartbeat datanode_heartbeat = new SendHeartbeat(datanodeID,namenodeAddress,namenodePortnumber);
            Thread heartBeatThread = new Thread(datanode_heartbeat);
            heartBeatThread.start();
            t1.start();
        }
    }
    
    
    /**
     * This method load data from a file and store it in a HashMap.
     * The data is a key value pair in the format of student ID and 
     * their names, major and year of graduation 
     */
    public synchronized void loadData(int datanodeID)
    {
      String line="";
        try 
        {
        
        //Read a particular line of data from the file 
        //The data line of data read is determined by the datanode id
        line = Files.readAllLines(Paths.get("data.txt")).get(datanodeID-1);
        } 
        catch (IOException ex) 
        {
         Logger.getLogger(Datanode.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        ///Split the data in into an array
        String[] dataArr = line.split(":");
     
        String key = dataArr[0];
        String value = dataArr[1];
        
        //Store the data in the HashMap
        this.nodeData.put(key,value);
    }
    
    /**
     * This method is used to find the id's of the data nodes to replicate data 
     * onto.
     * For instance, is the id of this data node is 1, it data will replicated 
     * on data nodes with id 2 and 3 if the replication factor is 2.
     */
    public void locator(int replicationFactor)
    {
        int inc =0;
        int locator=0;
        for (int i=0; i< replicationFactor;i++)
        {
            inc = i +1;
            locator = this.datanodeId + inc;
            if(locator == 6)
            {
                loadData(1);
            }
            else if(locator == 7)
            {
                loadData(2);
            }
            else if(locator == 8)
            {
                loadData(3);
            }
            
            else if(locator == 8)
            {
                loadData(4);
            }
            else
            {
                loadData(locator);
            }
    }
    }
            
    /**
     * This run method create a server socket connection to listen for connections 
     * from other data nodes. 
     * 
     * When a request is received, a socket is created. The run method then call
     * the create socket connection method to connect to the other data node.
     * 
     * This method also call the locator method to find data nodes to replicates it
     * data on. Replication is done only when number of data nodes available is five
     */
    
    public void run()
    {
        while(true)
        {
                try
                {
                    //ServerSocket datanodeServer = new ServerSocket(5000+ this.datanodeId);
                    Socket datanodeSocket = new Socket(this.namenode_address,this.namenode_portNumber);
                    PrintWriter out =
                        new PrintWriter(datanodeSocket.getOutputStream(), true);
                    BufferedReader in =
                        new BufferedReader(
                            new InputStreamReader(System.in));
                    BufferedReader br =
                        new BufferedReader(
                            new InputStreamReader(datanodeSocket.getInputStream()));
                    
                    String userinput;
                    
                    userinput = in.readLine();
                    
                    if(userinput.equalsIgnoreCase("end"))
                        {
                            out.println("die," + this.datanodeId);
                            System.out.println(br.readLine());
                            System.exit(0);
                        }    
                }
                catch(Exception e)
                {
                    System.err.println(e.toString());
                }
        }
    }
}
    
    /**
     * This is a sub class of the Data node class which implements the 
     * Runnable interface. It run method constantly sends heart beat messages 
     * to the names node class of the distributed system for the names node to be aware 
     * of its alive status. When is receives "noted" as a response from the 
     * name node, it close all of its socket connection to the name node as well as 
     * all the input and output stream it has opened. It then sleep the thread 
     * for one minutes. After it wakes up, the process of opening connection with 
     * the names node and sending heartbeat message is repeated.
     */
    class SendHeartbeat extends Datanode implements Runnable
    {
        //The IP address of the namenode
        //String namenodeAddress;
    
        //The port on which the namenode is always listening on
        //int namenodePortNumber;
       
        //The id of this datanode.
        //int datanodeId;
        
        /**
         * @param node_id The id of this data node
         * @param namenode_address The IP address of the name node to send heart beat 
         * messages to.
         * @param namenode_portNumber The port number on which the name node is 
         * listening for heartbeat messages from data nodes.
         */
        public SendHeartbeat(int node_id,String namenode_address,int namenode_portNumber)
        {
            super(node_id,namenode_address,namenode_portNumber);
            //this.datanodeId = node_id;
            //this.namenodeAddress = namenode_address;
            //this.namenodePortNumber = namenode_portNumber;
        }
            
        /**
         * This method override the run method of the Runnable interface.
         * 
         * It create a socket connection to the name node that manages all data nodes 
         * in the distributed system to send heart beat information.
         * 
         * When name node response ("noted"), the socket connection is closed and
         * the thread running the method is made to sleep for 30 seconds.
         * 
         * When the threads wake up, it create another connection to the name node
         * to send another heartbeat info repeating the whole process infinitely
         * 
         */
        @Override
        public void run()
        {
            boolean running = true;
            while(running)
            {
                try (
                    Socket datanodeHeartbeatSocket = new Socket(this.namenode_address, this.namenode_portNumber);
                    PrintWriter out =
                        new PrintWriter(datanodeHeartbeatSocket.getOutputStream(), true);
                    BufferedReader in =
                        new BufferedReader(
                            new InputStreamReader(datanodeHeartbeatSocket.getInputStream()));
                ) 
                {

                    //Send hearbeat message to the name node
                    out.println(datanodeId);

                    //Listens for namenode response to hearbeat  message sent
                    String namenodeResponse = in.readLine();
                    System.out.println(namenodeResponse);
                    //Analyze namesnode respose 
                    if(namenodeResponse.contains("Noted"))
                    {
                        String[] responseArr = namenodeResponse.split(",");
                        
                        try
                        {
                            int replicationFactor = Integer.parseInt(responseArr[0]);
                            this.locator(replicationFactor);
                        }catch(Exception e)
                        {
                            System.out.println(e.toString());
                        }
                    }
                    else if(namenodeResponse.equalsIgnoreCase("copy"))
                    {
                        System.out.println("Just received copy");
                        if(this.datanodeId == 1)
                        {
                            loadData(5);
                        }
                        else
                        {
                            loadData(this.datanodeId-1);
                        }
                    }
                    try
                    {
                        Thread.sleep(10000);
                    }
                    catch(Exception e)
                    {
                        System.err.println();
                    }
                } 
                catch (UnknownHostException e) 
                {
                    System.err.println("Don't know about namenode " + this.namenode_address);
                    System.err.println(e.toString());
                    System.exit(1);
                } 
                catch (IOException e) 
                {
                    System.err.println("Couldn't get I/O for the connection names node" +
                        this.namenode_address);
                    System.err.println(e.toString());
                    System.exit(1);
                }
            }
           
        }
}