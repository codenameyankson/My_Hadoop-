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
 * @author MrYankson
 */
public class NameNode {
    
    ServerSocket serverSocket = null;  
    int nameNode;
    HashMap metadata ;
    ArrayList activeNode;
    
    public  NameNode(){
         metadata = new HashMap();
         activeNode = new ArrayList();
         nameNode = 0;
        
    }
    
    
    public void listner(int portNumber){
      
        while (true){
           String input;
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
             
            while ((input = reader.readLine()) != null){
                if (input.equals(input)){
                 writer. println("Noted"); 
                 System.out.println(input);
                }
            }
               
        
    }   catch (IOException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
       }
    
}
}
