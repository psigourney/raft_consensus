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
// 4) If reply is serverId = -1; Send message to next server in list.  GOTO 2.

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
        String formattedMsg = "CLIENTMSG|" + message + "\n";
        try
        {
            Socket tcpSocket = new Socket(ipAddr, port);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);
            BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));
    
            outputWriter.write(formattedMsg);
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
        
        Scanner scanner = new Scanner(System.in);
        String input = "";
        
        while(true){
            System.out.print("Enter log update: ");
            
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
