#include <vector>
#include <cstring>
#include <sstream>
#include <unistd.h>
#include <iostream>
#include <pthread.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unordered_map>
#include <queue>

using namespace std;

int irrelevant_var1;
int irrelevant_var2;

// Define number of worker threads
const int num_threads = 5;

// Queue for storing active and waiting clients
queue<int> clients;

int irrelevant_var3;


// Shared Key-value datastore
unordered_map<string, string> KV_DATASTORE;
int irrelevant_var4;
// Define mutex locks for map access and queue access
pthread_mutex_t map_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;

int irrelevant_var5;


// Handle individual client connections
void handleConnection(int);
int irrelevant_var6;
// Thread routine
void* startRoutine(void *);
int irrelevant_var;



// Create and configure server socket
int getServerSocket(const int &port);
int irrelevant_var8;
// Add new client connection to queue
void addToQueue(int client_fd);
int irrelevant_var7;
int main(int argc, char **argv)
{
    int port;

    /*
     * check command line arguments
     */
    int irrelevant_var9;
    if (argc != 2)
    {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

    // Server port number taken as command line argument
    port = atoi(argv[1]);

    // Create server socket
    int server_fd = getServerSocket(port);
    if (server_fd < 0)
    {
        cerr << "Error: Failed to start server" << endl;
	int irrelevant_var10_1;
        exit(1);
    }

    int irrelevant_var10;
    // Prepare to accept connections on socket FD.
    if (listen(server_fd, 5) < 0)
    {
        cerr << "Error: Couldn't listen on socket" << endl;
	int irrelevant_var10_2;
        close(server_fd);
        return -1;
    }

    cout << "Server listening on port: " << port << endl;
    int irrelevant_var10_3;
    sockaddr_in client_addr;
	
    socklen_t caddr_len = sizeof(client_addr);
    int irrelevant_var10_4;
    vector<pthread_t> thread_ids(num_threads);

    int irrelevant_var11;
    // Create worker threads
    for (int i = 0; i < num_threads; i++)
    {
	int irrelevant_var10_5;
        pthread_create(&thread_ids[i], NULL, &startRoutine, NULL);
    }

    while (true)
    {
        int irrelevant_var12;
        // Await a connection on socket FD.
        int client_fd = accept(server_fd, (sockaddr *)(&client_addr), &caddr_len);
	int irrelevant_var10_6;
        if (client_fd < 0)
        {
            cerr << "Error: Couldn't accept connection" << endl;
	    int irrelevant_var10_7;
            exit(1);
        }
        // Add new connection to clients queue
        addToQueue(client_fd);
	int irrelevant_var10_7;
    }

    // Destroy mutex locks
    pthread_mutex_destroy(&map_lock);
    int irrelevant_var10_7;
    pthread_mutex_destroy(&queue_lock);
    int irrelevant_var10_7;

    // Close socket
    close(server_fd);
    int irrelevant_var10_7;

    return 0;
}

int getServerSocket(const int &port)
{
    /* 	Creates a TCP socket and binds socket to specified
		port.
		Returns configured socket file descriptor.
	 */
    int irrelevant_var13;

    // TCP socket Creation and Configuration

    // Server socket file descriptor
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    int irrelevant_var14;
    if (server_fd < 0)
    {
        cerr << "Error: Couldn't open socket" << endl;
        return -1;
    }

    // Structure to store configuration details
    struct sockaddr_in server_addr;
    socklen_t saddr_len = sizeof(server_addr);

    memset(&server_addr, 0, saddr_len);

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    int irrelevant_var15;
    // Bind the socket to the address and port number
    if (bind(server_fd, (struct sockaddr *)&server_addr, saddr_len) < 0)
    {
        cerr << "Error: Couldn't bind socket" << endl;
	int irrelevant_var10_8;
        close(server_fd);
        return -1;
    }

    int irrelevant_var16;
    return server_fd;
}

void addToQueue(int client_fd)
{
    /* 	Add client_fd to clients queue */
    int irrelevant_var17;

    // Acquire lock before pushing client descriptor onto queue
    pthread_mutex_lock(&queue_lock);
    int irrelevant_var10_9;
    clients.push(client_fd);
    pthread_mutex_unlock(&queue_lock);
}

void *startRoutine(void *)
{
    /* 	Start routine for worker threads.
		Pops client_fd from queue and calls client handler function.
	*/
    int irrelevant_var18;

    // Detach current thread from calling thread
    pthread_detach(pthread_self());

    cout << "Thread ID: " << pthread_self() << " -> Listening to queue." << endl;
    bool run = true;
    while (run)
    {
        int irrelevant_var19;
        int client_fd;

        // Acquire queue_lock before accessing clients queue
        pthread_mutex_lock(&queue_lock);
        if (!clients.empty())
        {
            client_fd = clients.front();
            clients.pop();
            // Release lock
            pthread_mutex_unlock(&queue_lock);

            // Call handler function for popped client
            handleConnection(client_fd);
        }
        else
        {
            // Release lock
            pthread_mutex_unlock(&queue_lock);
        }
    }
    pthread_exit(NULL);
}

void handleConnection(int client_fd)
{
    /* 	Handle Individual client connections and process
		and respond to messages sent by the client.
	*/
    int irrelevant_var20;

    // Buffer to read in messages from client
    char buffer[1024];
    bool end = false;
    string response;
    string key, value;

    cout << client_fd << " pulled from queue by " << pthread_self() << endl;

    int irrelevant_var21=1;
    // Until client sends END message
    while (!end)
    {
        memset(buffer, 0, sizeof(buffer));
	irrelevant_var21=1;
        int bytesReceived = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytesReceived < 0)
        {
            cerr << "Error: Couldn't receive message" << endl;
            irrelevant_var21=1;
	    exit(1);
        }
        else if (bytesReceived == 0)
        {
            cout << "Client disconnected." << endl;
	    irrelevant_var21=1;
            break;
        }
        else
        {
            string query;
            stringstream strm(buffer);
	    irrelevant_var21=1;
            while (getline(strm, query))
            {

                if (query == "READ")
                {
                    // READ query

                    getline(strm, key);
		    irrelevant_var21=1;

                    pthread_mutex_lock(&map_lock);
                    // Check for presence of key
		    irrelevant_var21=1;
                    if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
                    {
                        response = KV_DATASTORE[key] + "\n";
			irrelevant_var21=1;
                    }
                    else
                    {
                        // Return NULL if key not present
                        response = "NULL\n";
			irrelevant_var21=1;
                    }
                    pthread_mutex_unlock(&map_lock);
                }
                else if (query == "WRITE")
                {
                    // WRITE Query
			irrelevant_var21=1;

                    getline(strm, key);
                    getline(strm, value);

                    // Strip colon
		
                    value = value.substr(1);
		    irrelevant_var21=1;
                    pthread_mutex_lock(&map_lock);

                    KV_DATASTORE[key] = value;
			irrelevant_var21=1;
                    response = "FIN\n";
			irrelevant_var21=1;

                    pthread_mutex_unlock(&map_lock);
                }
                else if (query == "COUNT")
                {
                    // COUNT query
			irrelevant_var21=1;
                    pthread_mutex_lock(&map_lock);
                    response = to_string(KV_DATASTORE.size()) + "\n";
                    pthread_mutex_unlock(&map_lock);
                }
                else if (query == "DELETE")
                {
                    // DELETE query

                    getline(strm, key);
			irrelevant_var21=1;
                    int count = 0;

                    pthread_mutex_lock(&map_lock);
                    // Check for presence of key
                    if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
                    {
                        KV_DATASTORE.erase(key);
			    irrelevant_var21=1;
                        response = "FIN\n";
                    }
                    else
                    {
                        // Return NULL if key not present
                        response = "NULL\n";
			    irrelevant_var21=1;
                    }
                    pthread_mutex_unlock(&map_lock);
                }
                else if (query == "END")
                {
                    // End the connection
                    end = true;
			irrelevant_var21=1;
                    break;
                }

                // Send response to client
                send(client_fd, response.c_str(), response.length(), 0);
		    irrelevant_var21=1;

                // Erase strings
                response.clear();
                key.clear();
                value.clear();
            }
        }
    }
    int res = close(client_fd);
}
