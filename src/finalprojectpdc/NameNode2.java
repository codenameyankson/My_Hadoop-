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
             if (Integer.parseInt(args[0])<4)
                     {
              severSocket = new ServerSocket(portNumber); 
              System.out.println("Server Listening......");
                     }
             else 
             {
             System.out.println("Restart with a replication Factor less than 4");
             System.exit(0);
             }
            } catch (IOException ex) 
            {
                Logger.getLogger(NameNode2.class.getName()).log(Level.SEVERE, null, ex);
            }
            
           
            while(true)
            {
                try 
                {   
                    
                    nsocket = severSocket.accept();
                    System.out.println("connection Successful");
                    
                    MasterNameNode nameN=new MasterNameNode(nsocket,Integer.parseInt(args[0]));
                    nameN.start();
                 
                } catch (IOException ex) 
                {
                    Logger.getLogger(NameNode2.class.getName()).log(Level.SEVERE, null, ex);
                }

            
            }

    
        }

}

class MasterNameNode extends Thread
{
    public static int nodeCount = 5;
    private  int REPPLICATION_FACTOR;
    String dataLine = null;
    ServerSocket serverSocket = null;  
    int nameNodeID;
    private final HashMap metadata;
    public static ArrayList<Integer> activeNodes = new ArrayList();
    public static HashMap<Integer,Date> heartbeatTimestamp= new HashMap();
    BufferedReader  reader = null;
    PrintWriter writer = null;
    Socket clientSock = null;
    

public MasterNameNode(Socket cSocket, int replicationFactor)
{
        metadata = new HashMap<Integer,ArrayList<Integer>>();
        nameNodeID = 0;
        this.clientSock = cSocket;
        this.REPPLICATION_FACTOR = replicationFactor;
}

public synchronized void addNodeToArrayList(int node)
{
    this.getActiveNodes().add(node);
   
}

public synchronized ArrayList getActiveNodes()
{
    return this.activeNodes;
}


public void run()
{
        this.listner();
        
    
        
    
}

public ArrayList<Integer> dataLocation(int datanodeId){
    ArrayList<Integer> locations = new ArrayList();
    
   // int inc;
        int locator;
        for (int i=1; i<REPPLICATION_FACTOR+1;i++)
        {
            locator = datanodeId%this.nodeCount;
            locator +=i;
            System.out.println(locator);
            if (locator>5)
            {
               locations.add(locator%5);
            }
            else
            {
            locations.add(locator);
            }
        }
       
    return locations;
}
public void listner()
{

        int dataNodeId;
        try 
        {
            //this block is trying to initialize the reader and the writer 
            writer =new PrintWriter(clientSock.getOutputStream(), true);                   
            reader = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
                
        }catch (IOException ex) {
                 Logger.getLogger(MasterNameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            //the below statement keeps the server listening continuously 
                while (!"end".equals(dataLine = reader.readLine()))
                {
                    if (dataLine != null){
                        dataNodeId = Integer.parseInt(dataLine);
                        //inverted if statement to check if the datanodeID is not in the
                        //list of active nodes
                        if (! activeNodes.contains(dataNodeId))
                        {
                            //switch statement to identify our 5 Data nodes
                            switch (dataNodeId){
                                case 1:
                                    // adding to the active nodes list
                                    this.addNodeToArrayList(dataNodeId);
                                    this.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    //sending a response
                                    writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    // prinint to server console
                                    
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 2:
                                    this.addNodeToArrayList(dataNodeId);
                                   writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    this.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 3:
                                    this.addNodeToArrayList(dataNodeId);
                                   writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    this.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 4:
                                    this.addNodeToArrayList(dataNodeId);
                                    writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    this.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 5:
                                    this.addNodeToArrayList(dataNodeId);
                                    writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    this.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                default:
                                    writer. println("Node not Recognized");
                            }
                        }else
                        {
                            writer. println("node already in list");
                        }
                         for (int i =0; i<this.getActiveNodes().size(); i++)
                        {
                            System.out.print(this.metadata+"\n");
                            System.out.println(this.getActiveNodes().get(i) +" "+ this.getActiveNodes().size());
                        }
                        writer.flush();
                    }
                }
                //clientSock.close();
            }catch (SocketException i){
                System.out.println("Node just Died");
            }
        catch (IOException ex1) 
            {
                Logger.getLogger(MasterNameNode.class.getName()).log(Level.SEVERE, null, ex1);
            }catch(NullPointerException e)
            {
                                String line=this.getName(); 
                                System.out.println("Client "+line+" Closed");
             }
             catch(NumberFormatException n)
             {
                        //System.out.println("number format exception");
             }

            try{ this.writer.close();
                     this.reader.close();
                     this.clientSock.close();
            }catch (IOException i){
                         //noyhing yet
                 }

}

public void rebalance(int DeadNodeId)
{
    
    
}

public void createSocketConnecction(int datanodeID)
    {
        try(
            Socket recievingNodeSocket = new Socket("172.20.16.139",(5000+datanodeID)); 
            //PrintWriter out = new PrintWriter(recievingNodeSocket.getOutputStream());
            //BufferedReader in = new BufferedReader(new InputStreamReader(recievingNodeSocket.getInputStream()));
            DataInputStream in = new DataInputStream( recievingNodeSocket.getInputStream());
            DataOutputStream out = new DataOutputStream( recievingNodeSocket.getOutputStream());  
            )
        {
            //Send message to the other data node to copy data from the file
            out.writeUTF("replicate");
            
            String receivingNodeFeedback = in.readUTF();
            
            if(receivingNodeFeedback.equalsIgnoreCase("copied"))
            {
                in.close();
                out.close();
                recievingNodeSocket.close();
            }
        }
        catch(IOException ea)
        {
            System.err.println(ea.toString());
        }
    }



}