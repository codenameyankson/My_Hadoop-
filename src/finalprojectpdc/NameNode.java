/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalprojectpdc;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Kinsgey A - Yankson
 * @author 25/04/17
 */
public class NameNode {
    
    ServerSocket serverSocket = null;  
    int nameNode;
    HashMap metadata ;
    ArrayList activeNodes;
    
    public  NameNode(){
         metadata = new HashMap();
         activeNodes = new ArrayList();
         nameNode = 0;
        
    }
    
    
    public void listner(int portNumber)
    {
      
        while (true){
           int dataNodeId;
           try {
            serverSocket = new ServerSocket(portNumber);
        } catch (IOException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        if(serverSocket == null){
        
        System.out.println("Socket Not Created Due to PortNumber");
        }
        else{
            System.out.println("Server listening at "+portNumber);
        }
        try {
            Socket clientSock = serverSocket.accept();     
            PrintWriter writer =new PrintWriter(clientSock.getOutputStream(), true);                   
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
             
            while ((dataNodeId = Integer.parseInt(reader.readLine())) > 0){
                //inverted if statement to check if the datanodeID is not in the 
                //list of active nodes
                if (!activeNodes.contains(dataNodeId)){
                    //switch statement to identify our 5 Data nodes
                    switch (dataNodeId){
                        case 1:
                            // adding to the active nodes list
                            activeNodes.add(dataNodeId);
                            //sending a response 
                            writer. println("Noted"); 
                            // prinint to server console
                            System.out.println("Data node"+dataNodeId+" was added to the active nodes");
                        case 2:
                            activeNodes.add(dataNodeId);
                            writer. println("Noted"); 
                            System.out.println("Data node"+dataNodeId+" was added to the active nodes");
                        case 3:
                            activeNodes.add(dataNodeId);
                            writer. println("Noted"); 
                            System.out.println("Data node"+dataNodeId+" was added to the active nodes");
                        case 4:
                            activeNodes.add(dataNodeId);
                            writer. println("Noted"); 
                            System.out.println("Data node"+dataNodeId+" was added to the active nodes");
                        case 5:
                            activeNodes.add(dataNodeId);
                            writer. println("Noted"); 
                            System.out.println("Data node"+dataNodeId+" was added to the active nodes");
                        default:
                            writer. println("Node not Recognized");
                    }  
                    }else{
                            writer. println("Noted");
                }
                 
                }
    }   catch (IOException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
       }
}
}
