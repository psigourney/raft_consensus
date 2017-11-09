//Distributed Systems Fall 2017
//Raft Consensus Algorithm
//Patrick Sigourney & Howie Benefiel

//import com.google.gson.Gson;
//import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.*;

//Notes:
// java.util.timer for the heartbeat timer.
// If server is Follower:  spawn thread, create election timer, when AppendEntries RPC arrives, reset timer.

//AppendEntriesRPC Format:
// SendingServerIP|SendingServerPort|Leader'sTerm|Leader'sID|PreviousLogIndex|PreviousLogTerm|LogEntry(0 for heartbeat)|Leader'sCommittedIndex

class Node{
    public int id;
    public String ipAddr;
    public int port;
    
    public Node(int idParam, String ipAddrParam, int portParam){
        id = idParam;
        ipAddr = ipAddrParam;
        port = portParam;
    }
} 

class LogEntry{
    public int term;
    public String command;
    
    public LogEntry(int termParam, String commandParam){
        term = termParam;
        command = commandParam;
    }
}

class Listener implements Runnable{
    //Listen for messages
    //Message will either be an AppendEntries or RequestVote
    public static String myIp = "0";
    public static int myPort = 0;
    String[] msgArray;
    
    public Listener(String myIpParam, int myPortParam){
       myIp = myIpParam;
	   myPort = myPortParam;
	}
   
    public void run(){
        //while(1): Listen for TCP connection, receive message, call function to process message 
        try{
            ServerSocket tcpServerSocket = new ServerSocket(myPort);        
            Socket tcpClientSocket;
            while(true){
                tcpClientSocket = tcpServerSocket.accept();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpClientSocket.getInputStream()));
                PrintWriter outputWriter = new PrintWriter(tcpClientSocket.getOutputStream(), true);
                
                String inputLine = inputReader.readLine();
                if(inputLine.length() > 0){
                    System.out.println("MsgRcvd: " + inputLine);
                    
                    //Process the received message somehow
                    msgArray = inputLine.trim().split("|");
                    //Do stuff here.
                }
            }
        }
        catch(IOException ioe){System.err.println(myIp + ":" + myPort + ": " + ioe);}
    }
  
}




public class Server{
    public static int myServerId = 0;
    public static String myIp = "0";
    public static int myPort = 0;
    public static char serverRole = 'F'; //(L)eader, (C)andidate, or (F)ollower; initialized as Follower
    public static int term = 0;
    public static int leaderId = 1;  //Initialize to 0, setting to 1 for testing
    public static int votedFor = 0;
    
    //Timer electionTimer; when server is Follower, countdown to start election for new leader, receipt of heartbeat message will reset timer. (random time between 4-5 seconds)
    public static Timer electionTimer = new Timer();
    public static TimerTask startElectionTask = new TimerTask(){
        public void run(){
            //Do stuff here when timer expires.            
        }
    };
    
    
    //Timer heartbeatTimer; when server is Leader, timer to send heartbeat messages to other servers (2 seconds)
    // call with:  heartbeatTimer.scheduleAtFixedRate(startHeartbeatTask, startAfterMilliseconds, runEveryMilliseconds); (1000 = 1000ms = 1sec)
    public static Timer heartbeatTimer = new Timer();
    public static TimerTask startHeartbeatTask = new TimerTask(){
        public void run(){
            sendHeartbeat();
        }
    };
    

//AppendEntriesRPC Format:
// APPENDENTRY|SendingServerIP|SendingServerPort|Leader'sTerm|Leader'sID|PreviousLogIndex|PreviousLogTerm|LogEntry(0 for heartbeat)|Leader'sCommittedIndex    
    public static void sendHeartbeat(){
        for(Node node : nodeList){
            try{
            Socket tcpSocket = new Socket(node.ipAddr, node.port);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);

            String replyMessage =  "APPENDENTRY" + "|" + myIp + "|" + myPort;  //Will add more stuff later
                                   
            outputWriter.write(replyMessage + "\n");
            outputWriter.flush();
            tcpSocket.close();
            }
            catch(IOException ioe){//System.err.println(node.ipAddr + ":" + node.port + ": " + ioe); 
                                    continue;}
        }
    }
    
    

    public static ArrayList<Node> nodeList = new ArrayList<Node>();
    public static ArrayList<LogEntry> logList = new ArrayList<LogEntry>();
    
    public static void initialSetup(String filename) throws IOException {
        BufferedReader inputBuffer = new BufferedReader(new FileReader(filename));
        String[] lineArr = inputBuffer.readLine().trim().split("\\s+");
        int serverCount = -1;
        if(lineArr.length == 2){
            myServerId = Integer.parseInt(lineArr[0]);
            serverCount = Integer.parseInt(lineArr[1]);
        }
        else{
            System.out.println("Invalid input file");
            System.exit(-1);
        }
        
        for(int x = 0; x < serverCount; x++){
            lineArr = inputBuffer.readLine().split(":");
            if(lineArr.length == 3){
                if(Integer.parseInt(lineArr[0]) == myServerId){
                    myIp = lineArr[1];
                    myPort = Integer.parseInt(lineArr[2]);
                    continue; //nodeList should only contain the OTHER nodes.
                }
                nodeList.add(new Node(Integer.parseInt(lineArr[0]), lineArr[1], Integer.parseInt(lineArr[2])));
            }
            else{
                System.out.println("Invalid line in input file");
                System.exit(-1);
            }
        }
    }

    
    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.out.println("Invalid command line arguments");
            System.exit(-1);            
        }
        
        //Read in input file and setup nodes in nodeList
        initialSetup(args[0]); 
        
        //Spawn thread to listen for incoming messages
        Runnable listenerRunnable = new Listener(myIp, myPort);
        new Thread(listenerRunnable).start();
        
        //Testing
        //    for(Node line : nodeList) System.out.println("Node " + line.id + ": " + line.ipAddr + ":" + line.port);
            System.out.println("\nMy ServerId: " + myServerId);
            System.out.println("My IP: " + myIp);
            System.out.println("My Port: " + myPort);
        ////
        
        //If I'm the leader, send out a heartbeat every 2 seconds:
        if(myServerId == leaderId){
            heartbeatTimer.scheduleAtFixedRate(startHeartbeatTask, 2000, 2000);
        }
        
        
        return;
    }
    
}