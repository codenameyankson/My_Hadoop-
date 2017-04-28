/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalprojectpdc;

/**
 *
 * @author MrYankson
 */
import java.io.*;
import java.net.*;

public class Deprecated_DataNode {
    
     
    
    public void heartBeat(){
    
       
    try
     {
         
     Socket clientSocket = new Socket("192.168.12.81",1029); 
     if(clientSocket == null){
        
        System.out.println("Socket Not Created Due to PortNumber");
        }
        else{
            System.out.println("Server sending ");
        }
     
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            //BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
            
            writer.println("Alive");
            String nameNodeResponse = reader.readLine();
            System.out.println("echo: " + nameNodeResponse);
     }
    catch (UnknownHostException e) {
            System.err.println("Don't know about host " );
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to ");
            System.exit(1);
        
}
    
}
}
