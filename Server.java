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
import java.lang.reflect.Type;
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
    public int index;
    public String command;
    
    public LogEntry(int termParam, int indexParam, String commandParam){
        term = termParam;
        index = indexParam;
        command = commandParam;
    }
}

class Message{
    public String type;
    public int leaderTerm;  //Or candidate term if VOTEREQUEST
    public int leaderId;    //Or candidate ID if VOTEREQUEST
    public int prevLogIndex;
    public int prevLogTerm;
    public ArrayList<LogEntry> entries;
    public int leaderCommitIndex;
    public boolean reply;
    
    public Message( String typeParam,
                    int leaderTermParam,
                    int leaderIdParam,
                    int prevLogIndexParam,
                    int prevLogTermParam,
                    ArrayList<LogEntry> entriesParam,
                    int leaderCommitIndexParam,
                    boolean replyParam){
        type = typeParam;
        leaderTerm = leaderTermParam;
        leaderId = leaderIdParam;
        prevLogIndex = prevLogIndexParam;
        prevLogTerm = prevLogTermParam;
        entries = entriesParam;
        leaderCommitIndex = leaderCommitIndexParam;
        reply = replyParam;
    }
}

public class Server{
    
    //Timer values in milliseconds
    static final int HEARTBEAT_TIMER = 2000;    
    static final int MIN_ELECTION_TIMER = 5000; 
    static final int MAX_ELECTION_TIMER = 8000; 
    
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
    
    public static int commitIndex = 0; //Index of highest committed log entry
    public static int appliedIndex = 0; //Index of highest applied log entry

    //For Leaders to use:
    public static ArrayList<Integer> nextIndex; //for each server, index of next log entry to send to that server
    public static ArrayList<Integer> matchIndex; //for each server, index of highest log entry known to be replicated on that server

    
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
        ArrayList<LogEntry> myList = new ArrayList<LogEntry>();
        String message =  "APPENDENTRY" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm + "|" + leaderId;
        Message replyMsgObj = new Message("APPENDENTRY", currentTerm, leaderId, commitIndex, 0, myList, commitIndex, false);
        if(serverRole == 'L'){
            for(Node node : nodeList){
                sendMessage(replyMsgObj, node.id, node.ipAddr, node.port);
            }
        }
    }
    
    public static void sendVoteRequest(){
        ArrayList<LogEntry> myList = new ArrayList<LogEntry>();
        String message = "REQUESTVOTE" + "|" + myServerId + "|" + myIp + "|" + myPort + "|" + currentTerm;
        Message replyMsgObj = new Message("REQUESTVOTE", currentTerm, myServerId, 0, 0, myList, 0, true);
        if(serverRole == 'C'){
            for(Node node : nodeList){
                sendMessage(replyMsgObj, node.id, node.ipAddr, node.port);
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

                //initialize values
                logList.add(new LogEntry(0,0,"init"));
                nextIndex.add(0);
                matchIndex.add(0);

                
                nodeList.add(new Node(Integer.parseInt(lineArr[0]), lineArr[1], Integer.parseInt(lineArr[2])));
            }
            else{
                System.out.println("Invalid line in input file");
                System.exit(-1);
            }
        }
    }

 
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
  
    
    public static void listenForMessage() throws IOException {
        try{
            ServerSocket tcpServerSocket = new ServerSocket(myPort);        
            Socket tcpClientSocket;
            Type messageTypeToken = new TypeToken<Message>() {}.getType();
            
            while(true){
                tcpClientSocket = tcpServerSocket.accept();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpClientSocket.getInputStream()));
                String inputLine = inputReader.readLine();
                Gson gsonRecv = new Gson();
                Message receivedMessage = gsonRecv.fromJson(inputLine, messageTypeToken);

                    if(receivedMessage.type.equals("CLIENTMSG")){
                        System.out.println("CLIENTMSG RCVD");
                        if(serverRole == 'L'){
                            //Reply with leaderId value of 0 to indicate message recorded successfully.
                            Message replyMsgObj = new Message("CLIENTMSGREPLY", currentTerm, 0, 0, 0, receivedMessage.entries, commitIndex, true);
                            sendReply(replyMsgObj, 0, tcpClientSocket);  //send reply to client
                            
                            //  TODO: Send the AppendEntry message to all other servers.
                            
                            //  When at least 50% of servers have replied with TRUE, message is committed.
                            //  Add entry to the local log
                            commitIndex += 1;
                            logList.add(new LogEntry(currentTerm, commitIndex, receivedMessage.entries.get(0).command));
                            
                            //  Apply entry to the state machine
                            appliedIndex += 1;
                        }
                        else{
                            Message replyMsgObj;
                            if(leaderId == myServerId) 
                                //I don't know the real leader
                                replyMsgObj = new Message("CLIENTMSGREPLY", currentTerm, -1, 0, 0, receivedMessage.entries, commitIndex, false);
                            else
                                //The leader is leaderId
                                replyMsgObj = new Message("CLIENTMSGREPLY", currentTerm, leaderId, 0, 0, receivedMessage.entries, commitIndex, false);
                            sendReply(replyMsgObj, 0, tcpClientSocket);
                        }
                    }

                    electionTimer.cancel();
                    
                    if(receivedMessage.type.equals("APPENDENTRY")){
                        Message replyMsgObj;
                        if(receivedMessage.leaderTerm < currentTerm){
                            replyMsgObj = new Message("APPENDREPLY", currentTerm, leaderId, 0, 0, receivedMessage.entries, commitIndex, false);
                        }
                        else{
                            //Clear away any election stuff, in case it's been set
                            serverRole = 'F';
                            votedFor = 0;
                            votesReceived = 0;
                            currentTerm = receivedMessage.leaderTerm;
                            leaderId = receivedMessage.leaderId;
                            if(receivedMessage.reply == false){
                                //It's a heartbeat message
                                //Just send an empty reply with "true".
                                replyMsgObj = new Message("APPENDREPLY", currentTerm, leaderId, 0, 0, receivedMessage.entries, commitIndex, true);
                            }
                            else{
                                // TODO:  CHECK MY LOG and compare against teh message's log entries
                                // Craft reply accordingly

                                //Placeholder:
                                replyMsgObj = new Message("APPENDREPLY", currentTerm, leaderId, 0, 0, receivedMessage.entries, commitIndex, true);
                            }
                        }
                        sendReply(replyMsgObj, receivedMessage.leaderId, tcpClientSocket);
                    }
                    
                    else if(receivedMessage.type.equals("REQUESTVOTE")){
                        Message replyMsgObj;
                        if(receivedMessage.leaderTerm <= currentTerm || (votedFor != receivedMessage.leaderId && votedFor != 0 && currentTerm >= receivedMessage.leaderTerm)){
                            replyMsgObj = new Message("APPENDREPLY", currentTerm, leaderId, 0, 0, receivedMessage.entries, commitIndex, false);
                        }
                        else{
                            if(serverRole == 'L'){
                                serverRole = 'F';
                                heartbeatTimer.cancel();
                            }
                            currentTerm = receivedMessage.leaderTerm;
                            votedFor = receivedMessage.leaderId;
                            replyMsgObj = new Message("APPENDREPLY", currentTerm, votedFor, 0, 0, receivedMessage.entries, commitIndex, true);
                        }
                        sendReply(replyMsgObj, receivedMessage.leaderId, tcpClientSocket);
                    }
                    
                    
                    electionTimer = new Timer();
                    startElectionTask = new TimerTask(){ public void run(){startElection();}};
                    randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
                    electionTimer.schedule(startElectionTask, randomInt);
                
            }
        }
        catch(IOException ioe){System.err.println("***EXCEPTION: listenForMessage(): " + ioe);}
    }
    
    
    public static void sendMessage(Message msgObj, int serverId, String ipAddr, int port){
        Type messageTypeToken = new TypeToken<Message>() {}.getType();
        Gson gsonSend = new Gson();
        String stringMessage = gsonSend.toJson(msgObj, messageTypeToken);
        
        try{
            Socket tcpSocket = new Socket(ipAddr, port);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);
            BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));

            outputWriter.write(stringMessage + "\n");
            outputWriter.flush();
            
            String replyMessage = inputReader.readLine();
            
            Gson gsonRecv = new Gson();
            Message receivedReply = gsonRecv.fromJson(replyMessage, messageTypeToken);

            if(receivedReply.type.equals("APPENDREPLY")){
                if(receivedReply.leaderTerm  > currentTerm){
                    if(serverRole == 'L'){
                        heartbeatTimer.cancel();    
                    }
                    serverRole = 'F';  //Can't be a leader anymore, change to Follower
                    currentTerm = receivedReply.leaderTerm;
                }
            }
            else if(receivedReply.type.equals("VOTEREPLY")){    //Reply to REQUESTVOTE message
                if(receivedReply.reply == true){
                    votesReceived += 1;                            
                }
                else if(receivedReply.leaderTerm > currentTerm){
                    currentTerm = receivedReply.leaderTerm;
                    serverRole = 'F';
                }
            }
        }
        catch(IOException ioe){//System.err.println("sendMessage(): " + ipAddr + ":" + port + ": " + ioe);
                               return;}
    }
    

    public static void sendReply(Message replyMsgObj, int serverId, Socket tcpClientSocket){
        Type messageTypeToken = new TypeToken<Message>() {}.getType();
        Gson gsonSend = new Gson();
        String stringReply = gsonSend.toJson(replyMsgObj, messageTypeToken);
        
        try{
            PrintWriter outputWriter = new PrintWriter(tcpClientSocket.getOutputStream(), true);
            outputWriter.write(stringReply + "\n");
            outputWriter.flush();
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
        
        //electionTimer = new Timer();
        //startElectionTask = new TimerTask(){ public void run(){startElection();}};
        //randomInt = rand.nextInt((MAX_ELECTION_TIMER - MIN_ELECTION_TIMER) + 1) + MIN_ELECTION_TIMER;
        //electionTimer.schedule(startElectionTask, randomInt);

        listenForMessage();
    }
}


