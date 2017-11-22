//Distributed Systems Fall 2017
//Raft Consensus Algorithm
//Patrick Sigourney & Howie Benefiel

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.Math;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

//Compile with:  javac -cp .;gson-2.6.2.jar Server.java
//Run with: java -cp .;gson-2.6.2.jar Server

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

class Message{
    public String type;
    public int leaderTerm;
    public int prevLogIndex;
    public int prevLogTerm;
    public ArrayList<LogEntry> entries;
    public int leaderCommitIndex;
    
    public Message( String typeParam,
                    int leaderTermParam,
                    int prevLogIndexParam,
                    int prevLogTermParam,
                    ArrayList<LogEntry> entriesParam,
                    int leaderCommitIndexParam){
        type = typeParam;
        leaderTerm = leaderTermParam;
        prevLogIndex = prevLogIndexParam;
        prevLogTerm = prevLogTermParam;
        entries = entriesParam;
        leaderCommitIndex = leaderCommitIndexParam;
    }
}

public class Server{
    
    static final int HEARTBEAT_TIMER = 2000;    //milliseconds
    static final int MIN_ELECTION_TIMER = 5000; //milliseconds
    static final int MAX_ELECTION_TIMER = 8000; //milliseconds
    
    public static Random rand = new Random();   //For random election timer
    public static int randomInt = 0;            //For random election timer
    
    public static int numberOfServers = 0;
    public static int myServerId = 0;
    public static String myIp = "0";
    public static int myPort = 0;
    public static char serverRole = 'F';        //(L)eader, (C)andidate, or (F)ollower; initialized as Follower
    public static int currentTerm = 0;
    public static int leaderId = 0;
    public static int votedFor = 0;
    public static int votesReceived = 0;
    
    public static ArrayList<Node> nodeList = new ArrayList<Node>();
    public static ArrayList<LogEntry> logList = new ArrayList<LogEntry>();
    
    
    
    public static Timer electionTimer = new Timer();
    public static TimerTask startElectionTask = new TimerTask(){
        public void run(){
            startElection();            
        }
    };
    
    public static Timer heartbeatTimer = new Timer();
    public static TimerTask startHeartbeatTask = new TimerTask(){
        public void run(){
            sendHeartbeat();
        }
    };
    
   
   
    public static void sendHeartbeat(){
        String message =  "APPENDENTRY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + leaderId;
        if(serverRole == 'L'){
            for(Node node : nodeList){
                sendMessage(message, node.id, node.ipAddr, node.port);
            }
        }
    }
    
    public static void sendVoteRequest(){
        String message = "REQUESTVOTE" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm;
        if(serverRole == 'C'){
            for(Node node : nodeList){
                sendMessage(message, node.id, node.ipAddr, node.port);
            }
        }
    }
    
    
    
    public static void startElection(){
        electionTimer.cancel();
        serverRole = 'C';
        currentTerm += 1;
        votedFor = myServerId;
        votesReceived = 1;
        
        System.out.println("Election started for term " + currentTerm);
        sendVoteRequest();
        
        System.out.println("Votes Received for term " + currentTerm + ": " + votesReceived);
        if(votesReceived > Math.ceil(numberOfServers/2)){
            System.out.println("Elected leader for term " + currentTerm);
            heartbeatTimer.scheduleAtFixedRate(startHeartbeatTask, HEARTBEAT_TIMER, HEARTBEAT_TIMER);
            serverRole = 'L';
            leaderId = myServerId;
            votedFor = 0;
            votesReceived = 0;
        }
        else{
            serverRole = 'F';
            votedFor = 0;
            votesReceived = 0;
            electionTimer = new Timer();
            startElectionTask = new TimerTask(){ public void run(){startElection();}};
            randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
            electionTimer.schedule(startElectionTask, randomInt);
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
            Type messageTypeToken = new TypeToken<Message>() {}.getType();
            String[] msgArray;
            String replyMessage = "";
            
            
            //Encoding message into JSON:
            //Type messageTypeToken = new TypeToken<Message>() {}.getType();
            //Gson gsonSend = new Gson();
            //String stringData = gsonSend.toJson(messageObjectToSend, messageTypeToken);
            //outputWriter.write(stringData);
                        
            //Converting the message from JSON:
            //Type messageTypeToken = new TypeToken<Message>() {}.getType();
            //Gson gsonRecv = new Gson();
            //Message receivedMessage = gsonRecv.fromJson(receivedData, messageTypeToken);
            
            
            
            //Message object:
            //public String type;
            //public int leaderTerm;
            //public int prevLogIndex;
            //public int prevLogTerm;
            //public ArrayList<LogEntry> entries;
            //public int leaderCommitIndex;
            
            while(true){
                tcpClientSocket = tcpServerSocket.accept();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpClientSocket.getInputStream()));
                String inputLine = inputReader.readLine();
                Gson gsonRecv = new Gson();
                Message receivedMessage = gsonRecv.fromJson(receivedData, messageTypeToken);
                
                    // RESULT CODES for Client Messages
                    //      0 =  message sent
                    //      1..n = serverId of the leader (resend to leader)
                    //      -1 = leader is unknown
                    if(receivedMessage.messageType.equals("CLIENTMSG")){
                        System.out.println("CLIENTMSG RCVD");
                        if(serverRole == 'L'){
                            logList.add(new LogEntry(currentTerm, msgArray[1]));

                            replyMessage = "0\n";  //message logged successfully.
                            sendReply(replyMessage, 0, tcpClientSocket);
                            
                            //Now to send AppendLog message to all other servers.
                            
                            
                        }
                        else{
                            if(leaderId == myServerId) 
                                replyMessage = "-1\n"; //I don't know the real leader
                            //Send current leader info back to client
                            else
                                replyMessage = Integer.toString(leaderId) + "\n";
                            
                            sendReply(replyMessage, 0, tcpClientSocket);
                        }
                    }
                    
                    
                    if(msgArray.length < 5)     //Discard improperly formatted message
                        continue;
                    
                    electionTimer.cancel();

                    int senderServerId = Integer.parseInt(msgArray[1]);
                    String senderIp = msgArray[2];
                    int senderPort = Integer.parseInt(msgArray[3]);
                    int senderCurrentTerm = Integer.parseInt(msgArray[4]);
                    
                    if(messageType.equals("APPENDENTRY")){
                        if(senderCurrentTerm < currentTerm){
                            replyMessage =  "APPENDREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + "FALSE";                   
                        }
                        else{
                            serverRole = 'F';
                            votedFor = 0;
                            votesReceived = 0;
                            currentTerm = senderCurrentTerm;
                            leaderId = senderServerId;
                            replyMessage =  "APPENDREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + "TRUE";
                        }
                        sendReply(replyMessage, senderServerId, tcpClientSocket);
                    }
                    
                    else if(messageType.equals("REQUESTVOTE")){
                        if(senderCurrentTerm <= currentTerm || (votedFor != senderServerId && votedFor != 0 && currentTerm >= senderCurrentTerm)){
                            replyMessage = "VOTEREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + "FALSE";
                        }
                        else{
                            if(serverRole == 'L'){
                                serverRole = 'F';
                                heartbeatTimer.cancel();
                            }
                            currentTerm = senderCurrentTerm;
                            votedFor = senderServerId;
                            replyMessage = "VOTEREPLY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + "TRUE";
                        }
                        sendReply(replyMessage, senderServerId, tcpClientSocket);
                    }
                    
                    
                    electionTimer = new Timer();
                    startElectionTask = new TimerTask(){ public void run(){startElection();}};
                    randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
                    electionTimer.schedule(startElectionTask, randomInt);
                }
            }
        }
        catch(IOException ioe){System.err.println("***EXCEPTION: listenForMessage(): " + ioe);}
    }
    
    
    public static void sendMessage(String message, int serverId, String ipAddr, int port){
        try{
            Socket tcpSocket = new Socket(ipAddr, port);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);
            BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));

            outputWriter.write(message + "\n");
            outputWriter.flush();
            //System.out.println("MSG SENT: " + message);
            
            String replyMessage = inputReader.readLine();
            //System.out.println("RPLY RCVD: " + replyMessage);
            
            String[] msgArray = replyMessage.trim().split("\\|");
            if(msgArray.length < 5)  //Improperly formatted message; disregard.
                throw new IOException();
                
            String messageType = msgArray[0];
            int senderServerId = Integer.parseInt(msgArray[1]);
            String senderIp = msgArray[2];
            int senderPort = Integer.parseInt(msgArray[3]);
            int senderCurrentTerm = Integer.parseInt(msgArray[4]);
            
            if(messageType.equals("APPENDREPLY")){
                if(senderCurrentTerm > currentTerm){
                    serverRole = 'F';  //Can't be a leader anymore, change to Follower
                    currentTerm = senderCurrentTerm;
                }
            }
            else if(messageType.equals("VOTEREPLY")){    //Reply to REQUESTVOTE message
                if(msgArray[5].equals("TRUE")){
                    votesReceived += 1;                            
                }
                else if(senderCurrentTerm > currentTerm){
                    currentTerm = senderCurrentTerm;
                    serverRole = 'F';
                }
            }
        }
        catch(IOException ioe){//System.err.println("sendMessage(): " + ipAddr + ":" + port + ": " + ioe);
                               return;}
    }
    

    public static void sendReply(String message, int serverId, Socket tcpClientSocket){
    try{
        PrintWriter outputWriter = new PrintWriter(tcpClientSocket.getOutputStream(), true);
        outputWriter.write(message + "\n");
        outputWriter.flush();
        //System.out.println("RPLY SENT: " + message);
        }
        catch(IOException ioe){//System.err.println("sendMessage(): " + serverId + "; " + message + "; " + ioe);
                               return;}
    }
    
    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.out.println("Invalid command line arguments");
            System.exit(-1);            
        }
        initialSetup(args[0]);              //Read in input file and setup nodes
        
        ////////Informational
            System.out.println("\nMy ServerId: " + myServerId);
            System.out.println("My IP: " + myIp);
            System.out.println("My Port: " + myPort);
        ////////////////
        
        electionTimer = new Timer();
        startElectionTask = new TimerTask(){ public void run(){startElection();}};
        randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
        electionTimer.schedule(startElectionTask, randomInt);

        listenForMessage();
    }
}


