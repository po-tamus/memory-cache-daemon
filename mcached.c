#define _POSIX_C_SOURCE 199309L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "uthash.h"
#include "mcached.h"

#define PORT 11211
#define MAX_THREADS 128
#define BACKLOG 10

typedef struct cache_entry {
    char *key;
    void *value;
    size_t key_len; 
    size_t value_len;
    pthread_mutex_t lock;
    UT_hash_handle hh;
} cache_entry_t;

cache_entry_t *cache_table = NULL;
pthread_mutex_t table_mutex = PTHREAD_MUTEX_INITIALIZER;
int server_fd;

cache_entry_t *find_entry(const char *key, size_t key_len) {
    cache_entry_t *entry;
    HASH_FIND(hh, cache_table, key, key_len, entry);
    return entry;
}

void handle_get(int client_fd, memcache_req_header_t *hdr, uint8_t *key) {
    uint16_t key_len = ntohs(hdr->key_length);

    pthread_mutex_lock(&table_mutex);
    cache_entry_t *entry = find_entry((char *)key, key_len);
    if (entry) pthread_mutex_lock(&entry->lock);
    pthread_mutex_unlock(&table_mutex);

    memcache_req_header_t resp = {
        .magic = 0x81,
        .opcode = hdr->opcode,
        .vbucket_id = htons(entry ? RES_OK : RES_NOT_FOUND),
        .total_body_length = htonl(entry ? entry->value_len + key_len : 0),
    };

    write(client_fd, &resp, sizeof(resp));
    if (entry) {
        write(client_fd, key, key_len);
        write(client_fd, entry->value, entry->value_len);
        pthread_mutex_unlock(&entry->lock);
    }
}

void handle_set(int client_fd, memcache_req_header_t *hdr, uint8_t *key, uint8_t *value) {
    uint16_t key_len = ntohs(hdr->key_length);
    uint32_t total_len = ntohl(hdr->total_body_length);
    uint32_t value_len = total_len - key_len;

    pthread_mutex_lock(&table_mutex);
    cache_entry_t *entry = find_entry((char *)key, key_len);

    if (!entry) {
        entry = malloc(sizeof(cache_entry_t));
        entry->key = malloc(key_len);
        memcpy(entry->key, key, key_len);
        entry->key_len = key_len;
        pthread_mutex_init(&entry->lock, NULL);
        entry->value = NULL;
        entry->value_len = 0;
        HASH_ADD_KEYPTR(hh, cache_table, entry->key, key_len, entry);
    }

    pthread_mutex_lock(&entry->lock);
    pthread_mutex_unlock(&table_mutex);

    free(entry->value);
    entry->value = malloc(value_len);
    memcpy(entry->value, value, value_len);
    entry->value_len = value_len;

    pthread_mutex_unlock(&entry->lock);

    memcache_req_header_t resp = {
        .magic = 0x81,
        .opcode = hdr->opcode,
        .vbucket_id = htons(RES_OK),
        .total_body_length = htonl(0),
    };
    write(client_fd, &resp, sizeof(resp));
}

void handle_add(int client_fd, memcache_req_header_t *hdr, uint8_t *key, uint8_t *value) {
    uint16_t key_len = ntohs(hdr->key_length);
    uint32_t total_len = ntohl(hdr->total_body_length);
    uint32_t value_len = total_len - key_len;

    pthread_mutex_lock(&table_mutex);
    cache_entry_t *entry = find_entry((char *)key, key_len);

    if (entry) {
        pthread_mutex_unlock(&table_mutex);
        memcache_req_header_t resp = {
            .magic = 0x81,
            .opcode = hdr->opcode,
            .vbucket_id = htons(RES_EXISTS),
            .total_body_length = htonl(0),
        };
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    entry = malloc(sizeof(cache_entry_t));
    entry->key = malloc(key_len);
    memcpy(entry->key, key, key_len);
    entry->key_len = key_len;
    pthread_mutex_init(&entry->lock, NULL);
    entry->value = malloc(value_len);
    memcpy(entry->value, value, value_len);
    entry->value_len = value_len;
    HASH_ADD_KEYPTR(hh, cache_table, entry->key, key_len, entry);
    pthread_mutex_unlock(&table_mutex);

    memcache_req_header_t resp = {
        .magic = 0x81,
        .opcode = hdr->opcode,
        .vbucket_id = htons(RES_OK),
        .total_body_length = htonl(0),
    };
    write(client_fd, &resp, sizeof(resp));
}

void handle_delete(int client_fd, memcache_req_header_t *hdr, uint8_t *key) {
    uint16_t key_len = ntohs(hdr->key_length);

    pthread_mutex_lock(&table_mutex);
    cache_entry_t *entry = find_entry((char *)key, key_len);

    if (!entry) {
        pthread_mutex_unlock(&table_mutex);
        memcache_req_header_t resp = {
            .magic = 0x81,
            .opcode = hdr->opcode,
            .vbucket_id = htons(RES_NOT_FOUND),
            .total_body_length = htonl(0),
        };
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    pthread_mutex_lock(&entry->lock);
    HASH_DEL(cache_table, entry);
    pthread_mutex_unlock(&table_mutex);

    pthread_mutex_destroy(&entry->lock);
    free(entry->key);
    free(entry->value);
    free(entry);

    memcache_req_header_t resp = {
        .magic = 0x81,
        .opcode = hdr->opcode,
        .vbucket_id = htons(RES_OK),
        .total_body_length = htonl(0),
    };
    write(client_fd, &resp, sizeof(resp));
}

void handle_version(int client_fd, memcache_req_header_t *req_hdr) {
    const char *version = "C-Memcached 1.0";
    size_t len = strlen(version);

    memcache_req_header_t resp = {
        .magic = 0x81,
        .opcode = req_hdr->opcode,
        .vbucket_id = htons(RES_OK),
        .total_body_length = htonl(len)
    };

    write(client_fd, &resp, sizeof(resp));
    write(client_fd, version, len);
}

void handle_output(int client_fd, memcache_req_header_t *req_hdr) {
    pthread_mutex_lock(&table_mutex);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    cache_entry_t *entry, *tmp;
    for (entry = cache_table; entry != NULL; entry = tmp) {
        tmp = entry->hh.next;

        pthread_mutex_lock(&entry->lock);
        printf("%08lx:%08lx:", ts.tv_sec, ts.tv_nsec);
        for (size_t i = 0; i < entry->key_len; i++)
            printf("%02x", (unsigned char)entry->key[i]);
        printf(":");
        for (size_t i = 0; i < entry->value_len; i++)
            printf("%02x", ((unsigned char *)entry->value)[i]);
        printf("\n");
        pthread_mutex_unlock(&entry->lock);
    }

    pthread_mutex_unlock(&table_mutex);

    memcache_req_header_t resp = {
        .magic = 0x81,
        .opcode = req_hdr->opcode,
        .vbucket_id = htons(RES_OK),
        .total_body_length = htonl(0)
    };

    write(client_fd, &resp, sizeof(resp));
}

void send_error_response(int client_fd, uint8_t opcode) {
    memcache_req_header_t resp = {
        .magic = 0x81,
        .opcode = opcode,
        .vbucket_id = htons(RES_ERROR),
        .total_body_length = htonl(0),
    };
    write(client_fd, &resp, sizeof(resp));
}

void handle_client(int client_fd) {
    memcache_req_header_t hdr;
    ssize_t n = recv(client_fd, &hdr, sizeof(hdr), MSG_WAITALL);

    if (n != sizeof(hdr) || hdr.magic != 0x80) {
        send_error_response(client_fd, hdr.opcode);
        return;
    }

    uint32_t total_len = ntohl(hdr.total_body_length);
    uint16_t key_len   = ntohs(hdr.key_length);

    uint8_t *body = NULL;
    if (total_len > 0) {
        body = malloc(total_len);
        ssize_t body_read = recv(client_fd, body, total_len, MSG_WAITALL);
        if (body_read != total_len) {
            free(body);
            return;
        }
    }

    uint8_t *key = body;
    uint8_t *value = (key_len > 0) ? (body + key_len) : NULL;

    switch (hdr.opcode) {
        case CMD_GET:     handle_get(client_fd, &hdr, key); break;
        case CMD_SET:     handle_set(client_fd, &hdr, key, value); break;
        case CMD_ADD:     handle_add(client_fd, &hdr, key, value); break;
        case CMD_DELETE:  handle_delete(client_fd, &hdr, key); break;
        case CMD_VERSION: handle_version(client_fd, &hdr); break;
        case CMD_OUTPUT:  handle_output(client_fd, &hdr); break;
        default:          send_error_response(client_fd, hdr.opcode); break;
    }

    if (body) free(body);
}

void *worker_thread(void *arg) {
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (client_fd < 0) continue;
        handle_client(client_fd);
        close(client_fd);
    }
    return NULL;
}

int setup_server_socket(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = INADDR_ANY
    };

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(EXIT_FAILURE);
    }

    if (listen(sock, BACKLOG) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    return sock;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <port> <num_threads>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int port = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    if (num_threads <= 0 || num_threads > MAX_THREADS) {
        fprintf(stderr, "Invalid thread count. Max is %d.\n", MAX_THREADS);
        exit(EXIT_FAILURE);
    }

    server_fd = setup_server_socket(port);

    pthread_t threads[MAX_THREADS];
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, worker_thread, NULL);
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    close(server_fd);
    return 0;
}
