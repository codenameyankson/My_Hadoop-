/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Datanodes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author Ibrahim-Abdullah
 * @since 29-4-2017
 * @version version 1.0
 * 
 * 
 */
public class Datanode1 implements Runnable
{
    //The IP address of the namenode
    String namenode_address;
    
    //The port on which the namenode is always listening on
    int namenode_portNumber;
    
    //A hashmap to store data keys and values
    HashMap<String,ArrayList<Double>> nodeData = new HashMap<String,ArrayList<Double>>();
    
    
    /**
     * 
     * @param node_id The id of this data node
     * @param namenode_address The IP address of the name node to send heart beat 
     * messages to by the data node.
     * @param namenode_portNumber The port number on which the name node is listening
     * for heart beat messages from data nodes.
     */
    public Datanode1(int node_id,String namenode_address,int namenode_portNumber)
    {
        this.nodeData = null;
        this.namenode_address = namenode_address;
        this.namenode_portNumber = namenode_portNumber;
    }
    
    
    
    public void run()
    {
        
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
         * 
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
                    String heartbeatMessage = String.valueOf(datanodeId);

                    //Send hearbeat message to the name node
                    out.print(heartbeatMessage);

                    //Listens for namenode response to hearbeat  message sent
                    String namenodeResponse = in.readLine();

                    //Analyze namesnode respose 
                    if(namenodeResponse.equalsIgnoreCase("noted"))
                    {
                        in.close();
                        out.close();
                        datanodeHeartbeatSocket.close();
                        
                        try 
                        {
                            Thread.sleep((long)(0.5 * 1000));
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
}