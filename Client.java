//Distributed Systems Fall 2017
//Raft Consensus Algorithm
//Patrick Sigourney & Howie Benefiel

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

//Compile with:  javac -cp .;gson-2.6.2.jar Client.java
//Run with: java -cp .;gson-2.6.2.jar Client


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


public class Client{
    public static int numberOfServers = 0;
    public static ArrayList<Node> nodeList = new ArrayList<Node>();

    public static void initialSetup(String filename) throws IOException {
        BufferedReader inputBuffer = new BufferedReader(new FileReader(filename));
        String[] lineArr = inputBuffer.readLine().trim().split("\\s+");
        if(lineArr.length == 1){
            numberOfServers = Integer.parseInt(lineArr[0]);
        }
        else{
            System.out.println("Invalid input file");
            System.exit(-1);
        }
        
        for(int x = 0; x < numberOfServers; x++){
            lineArr = inputBuffer.readLine().split(":");
            if(lineArr.length == 3){
                nodeList.add(new Node(Integer.parseInt(lineArr[0]), lineArr[1], Integer.parseInt(lineArr[2])));
            }
            else{
                System.out.println("Invalid line in input file");
                System.exit(-1);
            }
        }
    }

// 1) Send message to first server in list.
// 2) If reply is OK, exit
// 3) If reply is redirect to another server, send message to the other server. GOTO 2.
// 4) If reply is serverId = 0; Send message to next server in list.  GOTO 2.

// RESULT CODES:
//      0 =  message sent
//      1..n = serverId of the leader (resend to leader)
//      -1 = leader is unknown

    public static int processUpdate(String message){
        int counter = -1;
        int result = sendUpdate(nodeList.get(0).ipAddr, nodeList.get(0).port, message);
        while(result != 0){
            if(result > 0){
                //Go to the server specified in the result.
                result = sendUpdate(nodeList.get(result-1).ipAddr, nodeList.get(result-1).port, message);
            }
            else{ 
                //Try next server in list
                int serverIndex = (++counter % numberOfServers);
                result = sendUpdate(nodeList.get(serverIndex).ipAddr, nodeList.get(serverIndex).port, message);
            }
        }
        return result;
    }
    

    public static int sendUpdate(String ipAddr, int port, String message){
        //Old & Busted:
        //String formattedMsg = "CLIENTMSG|" + message + "\n";
        
        //New Hotness:
        Message myMessage = new Message("CLIENTMSG", 0,0,0, new ArrayList<LogEntry>(new LogEntry(0,message)), 0);
                
        try
        {
            Type messageTypeToken = new TypeToken<Message>() {}.getType();
            Gson gsonSend = new Gson();
            String messageString = gsonSend.toJson(myMessage, messageTypeToken);
            
            Socket tcpSocket = new Socket(ipAddr, port);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);
            BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));
    
            outputWriter.write(messageString);
            outputWriter.flush();
    
            String replyMessage = inputReader.readLine();
            return Integer.parseInt(replyMessage.trim());
        }
        catch(IOException ioe){//System.err.println("sendUpdate(): " + ipAddr + ":" + port + "; " + ioe); 
                                return -1;}
    }

    
    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.out.println("Invalid command line arguments");
            System.exit(-1);            
        }
        
        //First line of file will be the number of servers, each subsequent line will be: serverId:serverIp:serverPort
        initialSetup(args[0]);
        
        String input = "";
        
        while(true){
            System.out.print("Enter log update: ");
            
            Scanner scanner = new Scanner(System.in);
            input = scanner.nextLine().trim();
            int result = -1;
            if(!input.equals("quit")){
                result = processUpdate(input);         
            }
            else System.exit(1);
            
            if(result == 0)
                System.out.println("Update delivered");
            else
                System.out.println("Delivery failed");
        }
    }
}
