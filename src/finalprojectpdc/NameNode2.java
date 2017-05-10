
package finalprojectpdc;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger; 

public class NameNode2{

	public static void main(String args[])
        {
            Socket nsocket=null;
            ServerSocket severSocket=null;
            int portNumber =4449;
         
            
            try 
            {
             
              severSocket = new ServerSocket(portNumber); 
              System.out.println("Server Listening......");
            } catch (IOException ex) 
            {
                Logger.getLogger(NameNode2.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            
            while(true){
                try 
                {
                    
                    nsocket = severSocket.accept();
                    System.out.println("connection Successful");
                    
                    MasterNameNode nameN=new MasterNameNode(nsocket,3);
                    nameN.start();
                    
                } catch (IOException ex) 
                {
                    Logger.getLogger(NameNode2.class.getName()).log(Level.SEVERE, null, ex);
                }

            
        }

    
	}

}

class MasterNameNode extends Thread{
    private  int REPPLICATION_FACTOR;
    String dataLine = null;
    ServerSocket serverSocket = null;  
    int nameNodeID;
    private final HashMap metadata;
    public static ArrayList<Integer> activeNodes = new ArrayList();
    public static HashMap<Integer,Date> heartbeatTimeStamp = new HashMap<>();
    BufferedReader  reader = null;
    PrintWriter writer = null;
    Socket clientSock = null;

public MasterNameNode(Socket cSocket, int replicationFactor){
	metadata = new HashMap();
        nameNodeID = 0;
	this.clientSock = cSocket;
        this.REPPLICATION_FACTOR = replicationFactor;
}

public synchronized void addNodeToArrayList(int node){
    this.getActiveNodes().add(node);
   
}

public synchronized ArrayList getActiveNodes()
{
    return this.activeNodes;
}


public void run(){

	this.listner();
}

public void listner(){

        int dataNodeId;
	try {
            //this block is trying to initialize the reader and the writer 
            writer =new PrintWriter(clientSock.getOutputStream(), true);                   
            reader = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
		
	}catch (IOException ex) {
            	Logger.getLogger(MasterNameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            //the below statement keeps the server listening continuously 
                while (!"end".equals(dataLine = reader.readLine())){
                    if (dataLine != null){
                        dataNodeId = Integer.parseInt(dataLine);
                        //inverted if statement to check if the datanodeID is not in the
                        //list of active nodes
                        if (! activeNodes.contains(dataNodeId)){
                            //switch statement to identify our 5 Data nodes
                            switch (dataNodeId){
                                case 1:
                                    // adding to the active nodes list
                                    this.addNodeToArrayList(dataNodeId);
                                    //sending a response
                                    writer. println("Noted");
                                    // prinint to server console
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 2:
                                    this.addNodeToArrayList(dataNodeId);
                                    writer. println("Noted");
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 3:
                                    this.addNodeToArrayList(dataNodeId);
                                    writer. println("Noted");
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 4:
                                    this.addNodeToArrayList(dataNodeId);
                                    writer. println("Noted");
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 5:
                                    this.addNodeToArrayList(dataNodeId);
                                    writer. println("Noted");
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                default:
                                    writer. println("Node not Recognized");
                            }
                        }else{
                            writer. println("Noted- node already in list");
                        }
                         for (int i =0; i<this.getActiveNodes().size(); i++){
                            System.out.println(this.getActiveNodes().get(i) +" "+ this.getActiveNodes().size());
                        }
                        writer.flush();
                    }
                }
                //clientSock.close();
            } catch (IOException ex1) {
                Logger.getLogger(MasterNameNode.class.getName()).log(Level.SEVERE, null, ex1);
            }catch(NullPointerException e){
			        String line=this.getName(); 
			        System.out.println("Client "+line+" Closed");
		    }

            try{ this.writer.close();
                     this.reader.close();
                     this.clientSock.close();
            }catch (IOException i){
                         //noyhing yet
                 }

}

public void reportActiveNodes()
{
    
    
}
}

class ActiveDatanodesTracker extends Thread
{
    HashMap<Integer,Date> heartbeatTimeStamp;
    
    public ActiveDatanodesTracker(HashMap<Integer,Date> hm, ArrayList<Integer> activenodes)
    {
        this.heartbeatTimeStamp = hm;
    }
}

