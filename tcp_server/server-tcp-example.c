

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <netdb.h>
#include <string.h> 
#include <unistd.h>
#include <fcntl.h>

/*
 * Server Main.
 */

#define MSG_SIZE 32

/* very OG way to pass arguments in C -- name args then provide the types */ 
int main(argc, argv)
int argc;
char **argv;
{
    unsigned short port;       /* port server binds to                */
    char buf[MSG_SIZE];              /* buffer for sending & receiving data */
    struct sockaddr_in client_addr; /* client address information          */
    struct sockaddr_in server_addr; /* server address information          */
    int orig_socket;                     /* socket for accepting connections    */
    int new_socket;                    /* socket connected to client          */
    int namelen;               /* length of client name               */
    int sleep_time;
    int keep_going;            /* flag to keep the server accepting connections from clients */ 
    /*
     * Check arguments. Should be only one: the port number to bind to.
     */

    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s port\n", argv[0]);
        exit(1);
    }

    /*
     * First argument should be the port.
     */
    port = (unsigned short) atoi(argv[1]);

    /*
     * Get a socket for accepting connections.
     */
    if ((orig_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("Socket()");
        exit(2);
    }

    /**
     * bind the socket to the server address 
     * let the OS pick the IP address 
     */
    server_addr.sin_family = AF_INET;
    server_addr.sin_port   = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;  

    if (bind(orig_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("Bind()");
        exit(3);
    }

    /*
     * Listen for connections. Specify the backlog as 1.
     */
    if (listen(orig_socket, 1) != 0)
    {
        perror("Listen()");
        exit(4);
    }

    /* add a while loop here to keep the server connecting to clients
     */
    keep_going = 1 ;   // flag I could change to zero to exit the server 
    while (keep_going ) { 
     
      /*
       * Accept a connection.
       */
      namelen = sizeof(client_addr);
      if ((new_socket = accept(orig_socket, (struct sockaddr *)&client_addr, &namelen)) == -1) {
      perror("Accept()");
      exit(5);
      }

      /*
       * Receive the message on the newly connected socket.
       */
      if (recv(new_socket, buf, sizeof(buf), 0) == -1) {
        perror("Recv()");
        exit(6);
      }

      printf("Server got message: %s \n",buf);
    
      /*
       * Send the message back to the client.
       */
      sleep_time = 1;
      printf("the server is on a lunch break for %d seconds \n",sleep_time); 
      sleep(sleep_time);
    
      if (send(new_socket, buf, sizeof(buf), 0) < 0) {
        perror("Send()");
        exit(7);
      }

      /* hack so the OS reclaims the port sooner 
	 Make the client close the socket first or the socket ends up in the TIME_WAIT state trying to close 
	 See: https://hea-www.harvard.edu/~fine/Tech/addrinuse.html
      */
    
      sleep(1);    
      close(new_socket);
      
    } // end keep_going */
    
    close(orig_socket);

    sleep(1);
    printf("Server ended successfully\n");
    exit(0);
}
