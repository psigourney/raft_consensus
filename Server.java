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
    
    static final int HEARTBEAT_TIMER = 2000;    //milliseconds
    static final int MIN_ELECTION_TIMER = 5000; //milliseconds
    static final int MAX_ELECTION_TIMER = 7000; //milliseconds
    
    public static Random rand = new Random();   //For random election timer
    public static int randomInt = 0;            //For random election timer
    
    public static int numberOfServers = 0;
    public static int myServerId = 0;
    public static String myIp = "0";
    public static int myPort = 0;
    public static char serverRole = 'F';        //(L)eader, (C)andidate, or (F)ollower; initialized as Follower
    public static int currentTerm = 0;
    public static int leaderId = 1;             //Initialize to 0, setting to 1 for testing
    public static int votedFor = 0;             //Who I voted for
    public static int votesReceived = 0;        //Number of votes I've received
    
    public static ArrayList<Node> nodeList = new ArrayList<Node>();
    public static ArrayList<LogEntry> logList = new ArrayList<LogEntry>();
    
    //When server is Follower, countdown to start election for new leader, receipt of heartbeat message will reset timer. 
    //(random time between 5-7 seconds)
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
    
   
    public static void sendHeartbeat(){
        for(Node node : nodeList){
            try{
            Socket tcpSocket = new Socket(node.ipAddr, node.port);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);

            String replyMessage =  "APPENDENTRY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + leaderId;  //Will add more stuff later
                                   
            outputWriter.write(replyMessage + "\n");
            outputWriter.flush();
            tcpSocket.close();
            }
            catch(IOException ioe){//System.err.println(node.ipAddr + ":" + node.port + ": " + ioe); 
                                    continue;}
        }
    }
    
    public static void startElection(){
        serverRole = 'C';       //Become a Candidate
        term += 1;              //Start the next term
        votedFor = myServerId;  //I voted for myself
        votesReceived = 1;      //I voted for myself
        
        electionTimer.cancel(); //We'll create a new one later
        sendVoteRequest();      //Send vote request to everyone
        
        electionTimer = new Timer();    //Here's the new timer.
        randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
        electionTimer.schedule(startElectionTask, randomInt);
    }

    public static void sendVoteRequest(){
        for(Node node : nodeList){
            try{
            Socket tcpSocket = new Socket(node.ipAddr, node.port);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);

            String replyMessage = "REQUESTVOTE" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm;
                                   
            outputWriter.write(replyMessage + "\n");
            outputWriter.flush();
            tcpSocket.close();
            }
            catch(IOException ioe){//System.err.println(node.ipAddr + ":" + node.port + ": " + ioe); 
                                    continue;}
        }
    }
    

    
    public static void initialSetup(String filename) throws IOException {
        BufferedReader inputBuffer = new BufferedReader(new FileReader(filename));
        String[] lineArr = inputBuffer.readLine().trim().split("\\s+");
        if(lineArr.length == 2){
            myServerId = Integer.parseInt(lineArr[0]);
            numberOfServers = Integer.parseInt(lineArr[1]);
        }
        else{
            System.out.println("Invalid input file");
            System.exit(-1);
        }
        
        for(int x = 0; x < numberOfServers; x++){
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
            String replyMessage = "";
            while(true){
                tcpClientSocket = tcpServerSocket.accept();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpClientSocket.getInputStream()));
                PrintWriter outputWriter = new PrintWriter(tcpClientSocket.getOutputStream(), true);
                
                String messageType = "0";
                int senderServerId = 0;
                String senderIp = "0";
                int senderPort = 0;
                int senderCurrentTerm = 0;
                
                String inputLine = inputReader.readLine();
                if(inputLine.length() > 0){
                    System.out.println("MsgRcvd: " + inputLine);
                    msgArray = inputLine.trim().split("|");
                    
                    if(msgArray.length < 5)  //Improperly formatted message; disregard.
                        continue;
                    
                    //Parse out the common message components
                    messageType = msgArray[0];
                    senderServerId = Integer.parseInt(msgArray[1]);
                    senderIp = msgArray[2];
                    senderPort = Integer.parseInt(msgArray[3]);
                    senderCurrentTerm = Integer.parseInt(msgArray[4]);
                    
                    
                    //"APPENDENTRY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + leaderId;
                    if(messageType.equals("APPENDENTRY")){
                        electionTimer.cancel();

                        replyMessage =  "APPENDREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm;

                        if(senderCurrentTerm < currentTerm){
                            replyMessage += "|FALSE";                            
                        }
                        else{
                            replyMessage += "|TRUE";
                            currentTerm = Integer.parseInt(msgArray[4]);
                            leaderId = senderServerId;
                        }
                        
                        sendMessage(replyMessage, senderIp, senderPort);
                        
                        electionTimer = new Timer();
                        randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
                        electionTimer.schedule(startElectionTask, randomInt);
                    }
                    
                    //"REQUESTVOTE" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm;
                    else if(messageType.equals("REQUESTVOTE")){
                        if(senderCurrentTerm <= currentTerm){
                            replyMessage = "VOTEREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + "FALSE";
                        }
                        else if(votedFor == 0){
                            //give vote
                            currentTerm = senderCurrentTerm;
                            votedFor = senderServerId;
                            replyMessage = "VOTEREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + "TRUE";
                        }
                        sendMessage(replyMessage, senderIp, senderPort);                        
                    }
                    
                    //"APPENDREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm;
                    else if(messageType.equals("APPENDREPLY"){
                        if(senderCurrentTerm > currentTerm){
                            serverRole = 'F';  //Can't be a leader anymore, change to Follower
                            currentTerm = senderCurrentTerm;
                        }
                    }
                    
                    //"VOTEREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + "TRUE/FALSE";
                    else if(messageType.equals("VOTEREPLY"){    //Reply to REQUESTVOTE message
                        if(msgArray[5].equals("TRUE"){
                            votesReceived += 1;                            
                        }
                        else if(senderCurrentTerm > currentTerm){
                            currentTerm = senderCurrentTerm;
                            serverRole = 'F';
                        }
                        
                    }
                }
            }
        }
        catch(IOException ioe){System.err.println("listenForMessage(): " + ioe);}
    }
    
    
    public static void sendMessage(String message, String ipAddr, int port){
        try{
            Socket tcpSocket = new Socket(ipAddr, port);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);

            outputWriter.write(message + "\n");
            outputWriter.flush();
            tcpSocket.close();
            }
            catch(IOException ioe){System.err.println(node.ipAddr + ":" + node.port + ": " + ioe);}
    }
    
    
    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.out.println("Invalid command line arguments");
            System.exit(-1);            
        }
                
        initialSetup(args[0]);              //Read in input file and setup nodes
        
        ////////Testing
        //    for(Node line : nodeList) System.out.println("Node " + line.id + ": " + line.ipAddr + ":" + line.port);
            System.out.println("\nMy ServerId: " + myServerId);
            System.out.println("My IP: " + myIp);
            System.out.println("My Port: " + myPort);
        ////////////////
        

        while(true){
            if(serverRole == 'L'){          //Leader sends regular heartbeat to all servers
                heartbeatTimer.scheduleAtFixedRate(startHeartbeatTask, HEARTBEAT_TIMER, HEARTBEAT_TIMER);
            }
            else if(serverRole == 'F'){          //Followers elect new leader if heartbeat not received
                randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
                electionTimer.schedule(startElectionTask, randomInt);
            }
            
            listenForMessage();             //Everyone listens for incoming messages
        }
    }
}


