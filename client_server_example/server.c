#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#define PORT 8080 

int main() { 
    int server_fd, new_socket; 
    struct sockaddr_in address; 
    char buffer[1024] = {0}; 
    char *message = "Hello from server"; 

    // fd
    server_fd = socket(AF_INET, SOCK_STREAM, 0); 

    // set the protofamily, address, and port
    address.sin_family = AF_INET; 
    address.sin_addr.s_addr = INADDR_ANY; 
    address.sin_port = htons(PORT); 

    // bind to port 
    bind(server_fd, (struct sockaddr *)&address, sizeof(address)); 
    listen(server_fd, 3); 

    // accept and read into buffer 
    int addrlen = sizeof(address); 
    new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen); 
    read(new_socket, buffer, 1024); 
    printf("Received: %s\n", buffer); 
    send(new_socket, message, strlen(message), 0); 

    close(new_socket); 
    close(server_fd); 
    return 0; 
}
