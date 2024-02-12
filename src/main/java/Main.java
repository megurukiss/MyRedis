import java.io.BufferedReader;

import java.io.DataOutputStream;

import java.io.IOException;

import java.io.InputStream;

import java.io.InputStreamReader;

import java.io.OutputStream;

import java.io.PrintWriter;

import java.net.ServerSocket;

import java.net.Socket;

public class Main {

    public static void main(String[] args) {

        // You can use print statements as follows for debugging, they'll be visible

        // when running tests.

        System.out.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;

        Socket clientSocket = null;

        int port = 6379;

        try {

            serverSocket = new ServerSocket(port);

            serverSocket.setReuseAddress(true);
            while (true) {

                clientSocket = serverSocket.accept();

                InputStream inputStream = clientSocket.getInputStream();

                BufferedReader inFromClient =

                        new BufferedReader(new InputStreamReader(inputStream));

                String clientCommand;

                // get the output stream from the socket.

                PrintWriter out = new PrintWriter(clientSocket.getOutputStream());

                while ((clientCommand = inFromClient.readLine()) != null) {

                    if (clientCommand.equalsIgnoreCase("PING")) {

                        out.print("+PONG\r\n");

                        out.flush();

                    }

                }

                //   out.close(); // close the output stream when we're done.

            }

        } catch (IOException e) {

            System.out.println("IOException: " + e.getMessage());
        } finally {

            try {

                if (clientSocket != null) {

                    clientSocket.close();

                }

            } catch (IOException e) {

                System.out.println("IOException: " + e.getMessage());

            }

        }

    }

}