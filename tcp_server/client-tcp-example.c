
/*
 * Include Files.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <netdb.h>
#include <string.h> 
#include <unistd.h>
#include <fcntl.h>

/*
 * Client Main.
 */
#define MSG_SIZE 32

int main(int argc, char ** argv) {
  
    unsigned short port;       /* port client will connect to         */
    char buf[MSG_SIZE];              /* data buffer for sending & receiving */
    struct hostent *hostnm;    /* server host name information        */
    struct sockaddr_in server; /* server address                      */
    int socketFD;              /* client socket file descriptor       */

    /*
     * Check Arguments Passed. Should be hostname and port.
     */
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s hostname port\n", argv[0]);
        exit(1);
    }

    /*
     * The host name is the first argument. Get the server address.
     */
    hostnm = gethostbyname(argv[1]);
    if (hostnm == (struct hostent *) 0)
    {
        fprintf(stderr, "Gethostbyname failed\n");
        exit(2);
    }

    /*
     * The port is the second argument.
     */
    port = (unsigned short) atoi(argv[2]);

    /*
     * Put a message into the buffer.
     */
    strcpy(buf, "Test message");

    /*
     * Put the server information into the server structure.
     * The port must be put into network byte order.
     */
    server.sin_family      = AF_INET;
    server.sin_port        = htons(port);
    server.sin_addr.s_addr = *((unsigned long *)hostnm->h_addr);

    /*
     * Get a stream socket.
     */
    if ((socketFD = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("Socket()");
        exit(3);
    }

    /*
     * Connect to the server. 
     */
    if (connect(socketFD, (struct sockaddr *)&server, sizeof(server)) < 0)
    {
        perror("Connect()");
        exit(4);
    }

    if (send(socketFD, buf, sizeof(buf), 0) < 0)
    {
        perror("Send()");
        exit(5);
    }

    /*
     * The server sends back the same message. Receive it into the
     * buffer.
     */
    if (recv(socketFD, buf, sizeof(buf), 0) < 0)
    {
        perror("Recv()");
        exit(6);
    }

    printf("The client got the message: %s \n",buf); 
    /*
     * Close the socket.
     */
    close(socketFD);
    // sleep(2);
    
    printf("Client Ended Successfully\n");
    exit(0);

}
