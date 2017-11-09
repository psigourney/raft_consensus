//Distributed Systems Fall 2017
//Raft Consensus Algorithm
//Patrick Sigourney & Howie Benefiel

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

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





public class Server{
    
    static final int HEARTBEAT_TIMER = 2000;     //milliseconds
    static final int MIN_ELECTION_TIMER = 5000;  //milliseconds
    static final int MAX_ELECTION_TIMER = 7000;  //milliseconds
    
    public static Random rand = new Random(); //For random election timer
    public static int randomInt = 7000;
    
    public static int myServerId = 0;
    public static String myIp = "0";
    public static int myPort = 0;
    public static char serverRole = 'F'; //(L)eader, (C)andidate, or (F)ollower; initialized as Follower
    public static int term = 0;
    public static int leaderId = 1;  //Initialize to 0, setting to 1 for testing
    public static int votedFor = 0;
    
    //When server is Follower, countdown to start election for new leader, receipt of heartbeat message will reset timer. (random time between 5-7 seconds)
    public static Timer electionTimer = new Timer();
    public static TimerTask startElectionTask = new TimerTask(){
        public void run(){
            startElection();            
        }
    };
    
    //When server is Leader, timer to send heartbeat messages to other servers (2 seconds)
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
    
    public static void startElection(){
        serverRole = 'C';
        term += 1;
        votedFor = myServerId;
        electionTimer.cancel();
        //Send RequestVote messages to all other servers   
        //If 1/2 vote for me, I'm the new leader
        
        electionTimer = new Timer();
        randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
        electionTimer.schedule(startElectionTask, randomInt);
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

    
    
    
    
    public static void listenForMessage() throws IOException {
        try{
            ServerSocket tcpServerSocket = new ServerSocket(myPort);        
            Socket tcpClientSocket;
            String[] msgArray;
            while(true){
                tcpClientSocket = tcpServerSocket.accept();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpClientSocket.getInputStream()));
                PrintWriter outputWriter = new PrintWriter(tcpClientSocket.getOutputStream(), true);
                
                String inputLine = inputReader.readLine();
                if(inputLine.length() > 0){
                    System.out.println("MsgRcvd: " + inputLine);
                    
                    //Process the received message somehow
                    msgArray = inputLine.trim().split("|");
                    if(msgArray[0].equals("APPENDENTRY")){
                        //cancel electionClock
                        electionTimer.cancel();
                        electionTimer = new Timer();
                        
                        //process AppendEntryRPC message
                        //update term, update leader

                        
                        //restart electionClock
                        //Set this number to random int between 5000-7000 (5-7secs)
                        randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
                        electionTimer.schedule(startElectionTask, randomInt);
                    }
                    if(msgArray[0].equals("REQUESTVOTE")){
                        //If requestedTerm < currentTerm, reply false
                        //If votedFor == 0 or votedFor == candidateId, give vote
                    }
                }
            }
        }
        catch(IOException ioe){System.err.println("listenForMessage(): " + ioe);}
    }
    
    
    
    
    
    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.out.println("Invalid command line arguments");
            System.exit(-1);            
        }
        
        //Read in input file and setup nodes in nodeList
        initialSetup(args[0]); 
        
        ////////Testing
        //    for(Node line : nodeList) System.out.println("Node " + line.id + ": " + line.ipAddr + ":" + line.port);
            System.out.println("\nMy ServerId: " + myServerId);
            System.out.println("My IP: " + myIp);
            System.out.println("My Port: " + myPort);
        ////////////////
        

        while(true){
            //If I'm the leader, send out a heartbeat every HEARTBEAT_TIMER seconds:
            if(myServerId == leaderId){
                heartbeatTimer.scheduleAtFixedRate(startHeartbeatTask, HEARTBEAT_TIMER, HEARTBEAT_TIMER);
            }
            if(serverRole == 'F'){  //If Follower and no heartbeat received for 5sec, start election to become Leader
                //Set timer to random int between 5
                randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
                electionTimer.schedule(startElectionTask, randomInt);
            }
            //Listen for incoming messages
            listenForMessage();
        }
        
        
    }
    
}