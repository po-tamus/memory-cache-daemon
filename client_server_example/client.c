#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT 8080

int main() {
    int sock; // file descriptor 
    struct sockaddr_in serv_addr; // address of the server 
    char *hello = "Hello from client"; // message 
    char buffer[1024] = {0}; // buffer to send

    sock = socket(AF_INET, SOCK_STREAM, 0); // create the tcp socket

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT); // port is known 
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    // connet 
    connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

    // send a message
    send(sock, hello, strlen(hello), 0);
    // read a message 
    read(sock, buffer, 1024);
    printf("Received: %s\n", buffer);

    close(sock);
    return 0;
}
