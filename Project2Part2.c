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
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>

#define MAX_EVENTS            64
#define DEFAULT_CLIENT_THREADS 4

/*
 * Frame definition for error control. The header contains:
 * - client_id : identifies the originating client.
 * - seq_num   : sequence number in the data packets.
 * - ack_num   : when sending an ACK this field holds the acknowledged sequence number.
 * - length    : payload length (if 0 then it is an ACK frame).
 *
 * The data (payload) follows the header.
 */
#define FRAME_PAYLOAD_SIZE 16

typedef struct {
    uint16_t client_id;
    uint16_t seq_num;    // used only in data frames (nonzero for new data)
    uint16_t ack_num;    // used only in ACK frames (set when length==0)
    uint16_t length;     // > 0 for data frames; 0 for ACK frames
    char payload[FRAME_PAYLOAD_SIZE];
} frame_t;

/*
 * Global server settings. In error-control, the server binds to the specified port.
 */
char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000; // number of new messages to send per thread

/*
 * Per-thread state for client threads.
 * Note: tx_cnt is incremented only upon a new (non-retransmitted) transmission.
 */
typedef struct {
    int socket_fd;             /* UDP socket descriptor */
    long long total_rtt;       /* Accumulated Round-Trip Time (in microseconds) */
    long total_messages;       /* Total successfully acknowledged messages */
    float request_rate;        /* Computed request rate (messages per second) */
    long tx_cnt;               /* Count of new (initial) transmissions */
    long rx_cnt;               /* Count of successful ACK receptions */
    uint16_t client_id;        /* Unique client ID (assigned from thread index) */
    uint16_t next_seq_num;     /* Next sequence number to send */
} client_thread_data_t;

/*
 * Client thread function. Each thread creates and sends messages embedded in frame_t
 * structures. If no ACK is received within the timeout, the message is retransmitted.
 */
void *client_thread_func(void *arg) {
    struct sockaddr_in server_addr;
    socklen_t addr_len = sizeof(server_addr);
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

    client_thread_data_t *data = (client_thread_data_t *)arg;
    
    // Initialize counters and sequence number
    data->tx_cnt = 0;
    data->rx_cnt = 0;
    data->total_rtt = 0;
    data->total_messages = 0;
    data->next_seq_num = 0;
    
    // Set the receive timeout for the socket (1 second)
    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    if (setsockopt(data->socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        perror("setsockopt failed");
    }
    
    // Initialize random number generator with a unique seed per thread
    unsigned int seed = time(NULL) ^ (pthread_self() * 1000);
    srand(seed);
    
    frame_t frame;
    frame_t ack_frame;
    struct timeval start, end;

    for (int i = 0; i < num_requests; i++){
        // Build the data frame to send
        memset(&frame, 0, sizeof(frame));
        frame.client_id = data->client_id;
        frame.seq_num   = data->next_seq_num;
        frame.ack_num   = 0;            // Not used in data frames
        frame.length    = FRAME_PAYLOAD_SIZE;
        // Generate random payload (printable ASCII characters)
        for (int j = 0; j < FRAME_PAYLOAD_SIZE; j++) {
            frame.payload[j] = (rand() % 95) + 32;
        }
        
        // Start timer for RTT measurement
        gettimeofday(&start, NULL);
        
        int retransmit = 0;
        while (1) {
            // On the first attempt, count the transmission; retransmissions do not update tx_cnt.
            if (!retransmit) {
                data->tx_cnt++;
            } else {
                printf("Client %d: Retransmitting packet with seq_num %d\n", data->client_id, frame.seq_num);
            }
            
            ssize_t sent = sendto(data->socket_fd, &frame, sizeof(frame), 0,
                                  (struct sockaddr *)&server_addr, sizeof(server_addr));
            if (sent < 0) {
                perror("sendto failed");
                break;
            }
            
            // Wait for the ACK frame from the server.
            memset(&ack_frame, 0, sizeof(ack_frame));
            ssize_t received = recvfrom(data->socket_fd, &ack_frame, sizeof(ack_frame), 0,
                                        (struct sockaddr *)&server_addr, &addr_len);
            if (received < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // Timeout occurred: no ACK received; set retransmit flag and try again.
                    retransmit = 1;
                    continue;
                } else {
                    perror("recvfrom failed");
                    break;
                }
            }
            
            // Validate the ACK: check client_id and that ack_num matches the sent seq_num.
            if (ack_frame.length == 0 && ack_frame.client_id == data->client_id && 
                ack_frame.ack_num == frame.seq_num) {
                gettimeofday(&end, NULL);
                long long rtt = (end.tv_sec - start.tv_sec) * 1000000LL +
                                  (end.tv_usec - start.tv_usec);
                data->total_rtt += rtt;
                data->total_messages++;
                data->rx_cnt++;
                // ACK received correctly; exit retransmission loop.
                break;
            } else {
                // Incorrect ACK? Continue waiting (or retransmit).
                printf("Client %d: Received incorrect ACK, expected seq_num %d but got %d\n",
                        data->client_id, frame.seq_num, ack_frame.ack_num);
                retransmit = 1;
            }
        }
        
        // Advance to the next sequence number.
        data->next_seq_num++;
    }
    
    // Compute request rate based on RTT and successful messages.
    data->request_rate = (data->total_messages > 0) ?
        ((float)data->total_messages / (float)data->total_rtt * 1000000) : 0;
    
    printf("Client %d - Sent: %ld, Acknowledged: %ld, Lost (after ARQ): %ld\n",
           data->client_id, data->tx_cnt, data->rx_cnt, data->tx_cnt - data->rx_cnt);
    
    return NULL;
}


/*
 * Server-side ARQ state: for each possible client, store the next expected sequence number.
 * We assume that client_id values are in the range [0, MAX_CLIENTS-1].
 */
#define MAX_CLIENTS 128
uint16_t expected_seq[MAX_CLIENTS] = {0};

/*
 * Server thread function using epoll to handle incoming UDP packets.
 * For each incoming data frame, the server checks the sequence number and sends an ACK.
 */
void run_server() {
    int server_socket, epoll_fd;
    struct sockaddr_in server_addr, client_addr;
    struct epoll_event event, events[MAX_EVENTS];
    socklen_t client_len = sizeof(client_addr);
    
    // Create UDP socket
    server_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_socket == -1) {
        perror("socket creation failed");
        exit(1);
    }
    
    // Set socket option to reuse address
    int reuse = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt failed");
        exit(1);
    }
    
    // Bind server address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(server_port);
    
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
    
    // Add server socket to epoll event list for reading incoming packets
    event.events = EPOLLIN;
    event.data.fd = server_socket;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_socket, &event) == -1) {
        perror("epoll_ctl failed");
        exit(1);
    }
    
    printf("Server listening on port %d\n", server_port);
    
    long long total_messages_received = 0;
    frame_t frame, ack_frame;
    
    // Main server loop
    while (1) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (num_events == -1) {
            perror("epoll_wait failed");
            continue;
        }
        
        for (int i = 0; i < num_events; i++) {
            if (events[i].data.fd == server_socket) {
                memset(&frame, 0, sizeof(frame));
                ssize_t bytes_received = recvfrom(server_socket, &frame, sizeof(frame), 0,
                                                  (struct sockaddr *)&client_addr, &client_len);
                if (bytes_received < 0) {
                    perror("recvfrom failed");
                    continue;
                }
                
                // Process only data frames (length > 0)
                if (frame.length > 0) {
                    total_messages_received++;
                    
                    uint16_t cid = frame.client_id;
                    uint16_t seq = frame.seq_num;
                    
                    if (cid >= MAX_CLIENTS) {
                        fprintf(stderr, "Received packet with invalid client_id %d\n", cid);
                        continue;
                    }
                    
                    // If the received sequence number is what we expect, update expected_seq.
                    if (seq == expected_seq[cid]) {
                        expected_seq[cid]++;
                    } else if (seq < expected_seq[cid]) {
                        // Duplicate packet received (likely a retransmission); do nothing extra.
                        printf("Server: Duplicate packet from client %d (seq: %d, expected: %d)\n", 
                               cid, seq, expected_seq[cid]);
                    } else {
                        // Out-of-order packet received. For Stop-and-Wait ARQ, this should not occur.
                        printf("Server: Out-of-order packet from client %d (seq: %d, expected: %d)\n", 
                               cid, seq, expected_seq[cid]);
                    }
                    
                    // Prepare and send an ACK frame.
                    memset(&ack_frame, 0, sizeof(ack_frame));
                    ack_frame.client_id = cid;
                    ack_frame.ack_num   = seq;
                    ack_frame.seq_num   = 0;      // Not used in ACK frames.
                    ack_frame.length    = 0;      // Zero-length indicates an ACK frame.
                    
                    ssize_t bytes_sent = sendto(server_socket, &ack_frame, sizeof(ack_frame), 0,
                                                (struct sockaddr *)&client_addr, client_len);
                    if (bytes_sent < 0) {
                        perror("sendto ACK failed");
                    } else {
                        // Optionally print ACK info.
                        printf("Server: Sent ACK for client %d, seq %d\n", cid, seq);
                    }
                }
            }
        }
    }
    
    // Cleanup (never reached in this infinite server loop)
    close(server_socket);
    close(epoll_fd);
}


/*
 * The run_client function creates multiple client threads.
 * Each client thread runs the error-control ARQ protocol.
 */
void run_client() {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];
    
    // Prepare server address (used by each client thread)
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    if(inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        perror("Invalid server IP address");
        exit(1);
    }
    
    // Create client threads.
    for (int i = 0; i < num_client_threads; i++){
        // Reset per-thread state.
        thread_data[i].total_rtt = 0;
        thread_data[i].total_messages = 0;
        thread_data[i].request_rate = 0;
        thread_data[i].client_id = i;  // assign unique id from thread index
        
        // Create a UDP socket.
        thread_data[i].socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (thread_data[i].socket_fd < 0) {
            perror("Socket creation failed");
            exit(1);
        }
        
        printf("Client thread %d created socket and attempting to connect to %s:%d\n",
               i, server_ip, server_port);
               
        // Note: Although UDP is connectionless, you may use connect() to bind a default peer.
        if (connect(thread_data[i].socket_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            perror("connect failed");
            exit(1);
        }
        
        if (pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]) != 0) {
            perror("pthread_create failed");
            exit(1);
        }
    }
    
    // Wait for client threads to finish and aggregate statistics.
    long long total_rtt = 0;
    long total_messages = 0;
    float total_request_rate = 0;
    
    for (int i = 0; i < num_client_threads; i++){
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
    printf("Overall Lost Packets (new transmissions not acknowledged): %ld\n", total_tx - total_rx);
    
    if (total_messages > 0){
        printf("Total Messages: %ld messages\n", total_messages);
        printf("Average RTT: %lld us (%lld ms)\n", total_rtt / total_messages, (total_rtt / total_messages) / 1000);
        printf("Aggregated Request Rate: %f messages/s\n", total_request_rate);
    } else {
        printf("No messages were successfully acknowledged.\n");
    }
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
