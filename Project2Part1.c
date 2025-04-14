/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/* 
Please specify the group members here
# Student #1: Coleman Earlywine
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>  // Add this for random number generation

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct {
        int socket_fd;       /* File descriptor for the client socket connected to the server. */
    long long total_rtt; /* Accumulated Round-Trip Time (RTT) for all messages sent and received (in microseconds). */
    long total_messages; /* Total number of messages sent and received. */
    float request_rate;  /* Computed request rate (requests per second) based on RTT and total messages. */
    long tx_cnt;
    long rx_cnt;
} client_thread_data_t;

/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) {
    struct sockaddr_in server_addr;
    socklen_t addr_len = sizeof(server_addr);
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    client_thread_data_t *data = (client_thread_data_t *)arg;
    
    // Initialize counters
    data->tx_cnt = 0;
    data->rx_cnt = 0;
    data->total_rtt = 0;
    data->total_messages = 0;
    
    struct timeval timeout;
    timeout.tv_sec = 1;  // Increase timeout to 1 second
    timeout.tv_usec = 0;
    setsockopt(data->socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

    // Initialize random number generator with a unique seed for each thread
    unsigned int seed = time(NULL) ^ (pthread_self() * 1000);
    srand(seed);
    
    char send_buf[MESSAGE_SIZE];
    char recv_buf[MESSAGE_SIZE];
    struct timeval start, end;

    // send/receive messages
    for (int i = 0; i < num_requests; i++){
        // Generate a random 16-byte message
        for (int j = 0; j < MESSAGE_SIZE; j++) {
            // Generate random printable ASCII characters (32-126)
            send_buf[j] = (rand() % 95) + 32;
        }
        
        gettimeofday(&start, NULL);

        // send message
        ssize_t sent = sendto(data->socket_fd, send_buf, MESSAGE_SIZE, 0, (struct sockaddr *)&server_addr, sizeof(server_addr));
        if (sent != -1) {
            data->tx_cnt++;
            
            // receive response
            memset(recv_buf, 0, MESSAGE_SIZE);
            ssize_t received = recvfrom(data->socket_fd, recv_buf, MESSAGE_SIZE, 0, (struct sockaddr *)&server_addr, &addr_len);
            
            if (received > 0) {
                data->rx_cnt++;
                
                // verify received message matches sent message
                if (memcmp(send_buf, recv_buf, MESSAGE_SIZE) != 0) {
                    printf("Received message does not match sent message\n");
                }
                
                // calculate RTT
                gettimeofday(&end, NULL);
                long long rtt = (end.tv_sec - start.tv_sec) * 1000000LL + 
                                (end.tv_usec - start.tv_usec);
                
                data->total_rtt += rtt;
                data->total_messages++;
            } else if (received == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // Timeout occurred, continue to next request
                    printf("Timeout waiting for response\n");
                } else {
                    perror("recv failed");
                    break;
                }
            }
        } else {
            perror("send failed");
            break;
        }
    }
 
    // calculate request rate
    data->request_rate = (data->total_messages > 0) ? 
        ((float)data->total_messages / (float)data->total_rtt * 1000000) : 0;

    printf("Thread sent: %ld, received: %ld, lost: %ld\n", data->tx_cnt, data->rx_cnt, data->tx_cnt - data->rx_cnt);
    
    return NULL;
}

/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each threads, and compute aggregated metrics of all threads.
 */
void run_client() {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];
    struct sockaddr_in server_addr;
    long long total_rtt = 0;
    long total_messages = 0;
    float total_request_rate = 0;
    
    // prepare server address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);

    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 500000;

    if(inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0){
        perror("Invalid Address");
        exit(1);
    }

    // create threads
    for(int i = 0; i < num_client_threads; i++){
        // reset thread data
        thread_data[i].total_rtt = 0;
        thread_data[i].total_messages = 0;
        thread_data[i].request_rate = 0;

        // create socket
        thread_data[i].socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
        if(thread_data[i].socket_fd == -1){
            perror("Socket Creation Failed");
            exit(1);
        }

        // connect to server
        printf("Attempting to connect to %s:%d\n", server_ip, server_port);
                printf("Connected successfully to thread %d\n", i);

        // create thread
        if(pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i])){
            perror("pthread_create failed");
            exit(1);
        }
    }

    // wait for threads
    for (int i = 0; i < num_client_threads; i++) {
       pthread_join(threads[i], NULL);

       total_rtt += thread_data[i].total_rtt;
       total_messages += thread_data[i].total_messages;
       total_request_rate += thread_data[i].request_rate;

       close(thread_data[i].socket_fd);
    }

    
    long total_tx = 0, total_rx = 0;
    for (int i = 0; i < num_client_threads; i++) {
        total_tx += thread_data[i].tx_cnt;
        total_rx += thread_data[i].rx_cnt;
    }
    printf("Overall Lost Packets: %ld\n", total_tx - total_rx);

    // print results
    if(total_messages > 0){
        printf("Total Messages: %ld messages\n",total_messages);
        printf("Average RTT: %lld us (%lld ms)\n", total_rtt / total_messages, (total_rtt / total_messages)/1000);
        printf("Total Request Rate: %f messages/s\n", total_request_rate);
    } else {
        printf("No messages were sent.\n");
    }
}

void run_server() {
    int server_socket, epoll_fd;
    struct sockaddr_in server_addr;
    struct epoll_event event, events[MAX_EVENTS];
    char recv_buf[MESSAGE_SIZE];
    char send_buf[MESSAGE_SIZE];
    long long total_messages_received = 0;

    // Create socket
    server_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_socket == -1) {
        perror("socket creation failed");
        exit(1);
    }

    // Set socket options
    int reuse = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt failed");
        exit(1);
    }

    // Prepare server address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(server_port);

    // Bind socket
    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        exit(1);
    }

    // Create epoll instance
    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1 failed");
        exit(1);
    }

    // Add server socket to epoll
    event.events = EPOLLIN;
    event.data.fd = server_socket;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_socket, &event) == -1) {
        perror("epoll_ctl failed");
        exit(1);
    }

    printf("Server listening on port %d\n", server_port);

    // Main event loop
    while (1) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (num_events == -1) {
            perror("epoll_wait failed");
            continue;
        }

        for (int i = 0; i < num_events; i++) {
            if (events[i].data.fd == server_socket) {
                // Handle incoming UDP packet
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                
                memset(recv_buf, 0, MESSAGE_SIZE);
                ssize_t bytes_received = recvfrom(server_socket, recv_buf, MESSAGE_SIZE, 0,
                                                  (struct sockaddr *)&client_addr, &client_len);
                
                if (bytes_received > 0) {
                    total_messages_received++;
                    
                    // Print received message
                    printf("Message #%lld - Received %zd bytes: ", total_messages_received, bytes_received);
                    for (int j = 0; j < bytes_received; j++) {
                        printf("%c", (recv_buf[j] >= 32 && recv_buf[j] <= 126) ? recv_buf[j] : '.');
                    }
                    printf("\n");
                    
                    // Echo back
                    ssize_t bytes_sent = sendto(server_socket, recv_buf, bytes_received, 0,
                                               (struct sockaddr *)&client_addr, client_len);
                    if (bytes_sent == -1) {
                        perror("send failed");
                    } else {
                        printf("Echoed %zd bytes back to client\n", bytes_sent);
                    }
                }
            }
        }
    }

    // Cleanup
    close(server_socket);
    close(epoll_fd);
}

int main(int argc, char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "server") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);

        run_server();
    } else if (argc > 1 && strcmp(argv[1], "client") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        if (argc > 4) num_client_threads = atoi(argv[4]);
        if (argc > 5) num_requests = atoi(argv[5]);

        run_client();
    } else {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }

    return 0;
}
