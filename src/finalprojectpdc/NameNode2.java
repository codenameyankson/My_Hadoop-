package finalprojectpdc;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger; 

public class NameNode2{
    /**
    *class for the Name node. 
     * @param args
    @args Replication factor
    */
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
/**
 * @Ibrahim
 * @author MrYankson
 * This is a class modeling the masterNode in the hadoop architecture.
 */
class MasterNameNode extends Thread
{
    public static int nodeCount = 5;        //the Number of desired nodes for Successful startup
    private  int REPPLICATION_FACTOR;       //replication factor for each data in the system
    ServerSocket serverSocket = null;       //Server socket 
    int nameNodeID;
    public static  HashMap metadata;         // a map containng the datanodes and the data stored on them
    public static ArrayList<Integer> activeNodes = new ArrayList();     //list keeping a track of our active nodes
   
    static boolean isAlive = true;
    BufferedReader  reader = null;
    PrintWriter writer = null;
    Socket clientSock = null;
    String dataLine = null;

    /**
    Constructor for the Name Node class
    @param socket client socket
    * @param int replication factor
    */
public MasterNameNode(Socket cSocket, int replicationFactor)
{
        metadata = new HashMap<Integer,ArrayList<Integer>>();
        nameNodeID = 0;
        this.clientSock = cSocket;
        this.REPPLICATION_FACTOR = replicationFactor; // replication factor detemined by the client on start up via constructor
}
/**
 * Synchronized method used by threads to add to the static array list 
 * @param node 
 */
public synchronized void addNodeToArrayList(int node)
{
    this.getActiveNodes().add(node);
   
}
/***
 * 
 * @return an ArrayList of active nodes 
 */
public synchronized ArrayList getActiveNodes()
{
    return this.activeNodes;
}
/**
 * this is the run method for the class that runs a thread to listen to all 
 * connections.
 */

public void run()
{
        this.listner();   
}
/**
 * this method calculates the datanodes where a particular node's data should
 * be copied to. based on the replication factor.
 * @param datanodeId
 * @return ArrayList of locations
 */
public ArrayList<Integer> dataLocation(int datanodeId){
    ArrayList<Integer> locations = new ArrayList();
    locations.add(datanodeId);
   // int inc;
        int locator;
        for (int i=1; i<REPPLICATION_FACTOR+1;i++)
        {
            locator = datanodeId%this.nodeCount;
            locator +=i;
            //System.out.println(locator);
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

public void report()
{
    //FileReader fr = new FileReader("data.txt");
    Iterable<Integer> dataNodelist =  MasterNameNode.metadata.keySet();
    
    for (Integer key : dataNodelist)
    {
        System.out.println("\n \tThis is report for Data on node:" + key+"\n");
        ArrayList<Integer> valueList = (ArrayList<Integer>)MasterNameNode.metadata.get(key);
            
        for (int ln : valueList)       
        {
            try 
            {  System.out.println("___________________________________________");
                URL path = MasterNameNode.class.getResource("data.txt");
                String a = path.getPath();
                String  line = Files.readAllLines(Paths.get(a)).get(ln-1);
                System.out.println(line);
            
            } catch (IOException ex)
            {
                Logger.getLogger(MasterNameNode.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
 
    
    }   
}





/**
 * This method continues to listen for messages from the clients and initiates 
 * a command based on the message sent.
 */
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
                    // this line below detects failure of a node
                    if(dataLine.contains("die")) 
                    {  
                        --MasterNameNode.nodeCount;         //this is to accomodate for dead node
                        MasterNameNode.isAlive = false;
                        System.out.println("Datanode just died");
                        writer.println("Replicating data");
                        String [] command = dataLine.split(",");
                        int deadNodeId = Integer.parseInt(command[1]);
                        MasterNameNode.activeNodes.clear();
                        MasterNameNode.metadata.clear();
                        //call replicate command
                    }
                    
                    else if (dataLine != null)
                    {
                        dataNodeId = Integer.parseInt(dataLine);
                        //inverted if statement to check if the datanodeID is not in the
                        //list of active nodes
                        if (! activeNodes.contains(dataNodeId) && MasterNameNode.isAlive)
                        {
                            //switch statement to identify our 5 Data nodes
                            switch (dataNodeId){
                                case 1:
                                    // adding to the active nodes list
                                    this.addNodeToArrayList(dataNodeId);
                                    MasterNameNode.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    //sending a response
                                    writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    // prinint to server console
                                    
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 2:
                                    this.addNodeToArrayList(dataNodeId);
                                   writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    MasterNameNode.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 3:
                                    this.addNodeToArrayList(dataNodeId);
                                   writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    MasterNameNode.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 4:
                                    this.addNodeToArrayList(dataNodeId);
                                    writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    MasterNameNode.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                case 5:
                                    this.addNodeToArrayList(dataNodeId);
                                    writer. println(this.REPPLICATION_FACTOR +",Noted ");
                                    MasterNameNode.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                    System.out.println("Data node "+dataNodeId+" was added to the active nodes");
                                    break;
                                default:
                                    writer. println("Node not Recognized");
                            }
                        }
                        else
                        {
                             if (!MasterNameNode.isAlive && !this.activeNodes.contains(dataNodeId))
                             {
                                  
                                writer.println("copy");
                                this.addNodeToArrayList(dataNodeId);
                                MasterNameNode.metadata.put(dataNodeId,dataLocation(dataNodeId));
                                if(dataNodeId ==1)
                                {
                                   ArrayList<Integer> l =  ( ArrayList<Integer> )MasterNameNode.metadata.get(dataNodeId);
                                           l.add(5);
                                }
                                else
                                {
                                    ArrayList<Integer> l =  ( ArrayList<Integer> )MasterNameNode.metadata.get(dataNodeId);
                                           l.add(dataNodeId-1);
                                }
                                
                                if(MasterNameNode.activeNodes.size()==MasterNameNode.nodeCount)
                                {
                                    MasterNameNode.isAlive = true;
                                   
                                }
                                 this.report();
                             }
                             else
                             {
                                   writer. println("node already in list");
                             } 
                        }
                       // this.report();
                        System.out.print("\n"+ MasterNameNode.metadata+"\n");
//                        for (int i =0; i<this.getActiveNodes().size(); i++)
//                        {
//                            
//                            System.out.println(this.getActiveNodes().get(i) +" "+ this.getActiveNodes().size());
//                        }
                        writer.flush();
                    }
                }
            }catch (SocketException i){
                System.out.println("Node just Died");
            }
        catch (IOException ex1) 
            {
                Logger.getLogger(MasterNameNode.class.getName()).log(Level.SEVERE, null, ex1);
            }catch(NullPointerException e)
            {
                                //String line=this.getName(); 
                               // System.out.println("Client "+line+" Closed");
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




}
