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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
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
    HashMap<String,String> nodeData;
    
    
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
        this.nodeData = new HashMap();
        this.serverSocket = null;
        this.datanodeId = node_id;
        this.namenode_address = namenode_address;
        this.namenode_portNumber = namenode_portNumber;
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
            
            Datanode datanode = new Datanode(datanodeID,namenodeAddress,namenodePortnumber);
            //Thread datanodeListener= new Thread(datanode);
           // datanodeListener.start();
           
           datanode.loadData();
            SendHeartbeat datanode_heartbeat = new SendHeartbeat(datanodeID,namenodeAddress,namenodePortnumber);
            Thread heartBeatThread = new Thread(datanode_heartbeat);
            heartBeatThread.start();
        }
    }
    /**
     * This method load data from a file to be stored in a HashMap.
     * The data is a key value pair.
     */
    public void loadData()
    {
      String line="";
        try {
        line = Files.readAllLines(Paths.get("data.txt")).get(datanodeId-1);
     } catch (IOException ex) {
         Logger.getLogger(Datanode.class.getName()).log(Level.SEVERE, null, ex);
    }
        //String line = "4002018:Ibrahim Abdulllah";
        String[] dataArr = line.split(":");
     
        String key = dataArr[0];
        String value = dataArr[1];
        System.out.println(key+value);
        this.nodeData.put(key,value);
        
        System.out.println(nodeData.get(key));
    }
    
    /**
     * 
     */
    public void locator ()
    {
        int inc =0;
        int locator=0;
        for (int i=0; i<this.datanodeId;i++)
        {
            inc = i +1;
            locator = this.datanodeId + inc;
            //tranferFunction(locator);
        }
    }
    
    
    public int yearSpentInSchool()
    {
        
    }
    public void run()
    {
        
    }
  
    /**
     * Think about how to send the data
     * @param dataKey
     * @param othernodes_listenPortNumber
     * @return 
     */
    public String copyData(String dataKey,int othernodes_listenPortNumber)
    {
         return "success";
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
    class SendHeartbeat implements Runnable
    {
        //The IP address of the namenode
        String namenodeAddress;
    
        //The port on which the namenode is always listening on
        int namenodePortNumber;
       
        //The id of this datanode.
        int datanodeId;
        
        /**
         * @param node_id The id of this data node
         * @param namenode_address The IP address of the name node to send heart beat 
         * messages to.
         * @param namenode_portNumber The port number on which the name node is 
         * listening for heartbeat messages from data nodes.
         */
        public SendHeartbeat(int node_id,String namenode_address,int namenode_portNumber)
        {
            this.datanodeId = node_id;
            this.namenodeAddress = namenode_address;
            this.namenodePortNumber = namenode_portNumber;
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
            while(true)
            {
                try (
                    Socket datanodeHeartbeatSocket = new Socket(this.namenodeAddress, this.namenodePortNumber);
                    PrintWriter out =
                        new PrintWriter(datanodeHeartbeatSocket.getOutputStream(), true);
                    BufferedReader in =
                        new BufferedReader(
                            new InputStreamReader(datanodeHeartbeatSocket.getInputStream()));
                ) 
                {
                    //String heartbeatMessage = String.valueOf(datanodeId);

                    //Send hearbeat message to the name node
                    out.println(Integer.toString(datanodeId));

                    //Listens for namenode response to hearbeat  message sent
                    String namenodeResponse = in.readLine();
                        
                    System.out.println(namenodeResponse);
                    //Analyze namesnode respose 
                    if(namenodeResponse.equalsIgnoreCase("noted"))
                    {
                        out.println();
                        in.close();
                        out.close();
                        datanodeHeartbeatSocket.close();
                        
                        try 
                        {
                            Thread.sleep((long)(100000));
                        } 
                        catch (InterruptedException e) 
                        {
                            System.err.println(e.toString());
                        }
                    }
                } 
                catch (UnknownHostException e) 
                {
                    System.err.println("Don't know about namenode " + this.namenodeAddress);
                    System.err.println(e.toString());
                    System.exit(1);
                } 
                catch (IOException e) 
                {
                    System.err.println("Couldn't get I/O for the connection names node" +
                        this.namenodeAddress);
                    System.err.println(e.toString());
                    System.exit(1);
                }
            }
           
        }
}