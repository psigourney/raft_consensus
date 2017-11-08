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
import java.util.*;

//Notes:
// java.util.timer for the heartbeat timer.
// If server is Follower:  spawn thread, create election timer, when AppendEntries RPC arrives, reset timer.


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
    public static int  myServerId = -1;
    public static char serverRole = 'X'; //(L)eader, (C)andidate, or (F)ollower
    public static int  term = -1;
    public static int  leaderId = -1;
    public static int  votedFor = -1;

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
                nodeList.add(new Node(Integer.parseInt(lineArr[0]), lineArr[1], Integer.parseInt(lineArr[2])));
            }
            else{
                System.out.println("Invalid line in input file");
                System.exit(-1);
            }
        }
        System.out.println(nodeList.size() + " nodes added to nodeList");
        
    }
    
    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.out.println("Invalid command line arguments");
            System.exit(-1);            
        }
        initialSetup(args[0]); //Read in input file and setup nodes in nodeList
        
        //Testing
            for(Node line : nodeList) System.out.println("Node " + line.id + ": " + line.ipAddr + ":" + line.port);
        ////
        
        return;
    }
    
}