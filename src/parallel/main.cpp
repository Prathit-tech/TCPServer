#include <vector>
#include <cstring>
#include <sstream>
#include <unistd.h>
#include <iostream>
#include <pthread.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unordered_map>

using namespace std;

pthread_mutex_t lock;

// Function prototypes
int getServerSocket(const int &port);
void* handleConnection(void *);
int var1= 0;
void processRequest(const string& query, const string& key, const string& value, const int& client_socket);
// Function to send response to client
void sendResponse(const string& server_response, const int& client_socket);

unordered_map<string, string> KV_DATASTORE;
int client_number = 0;
int main(int argc, char **argv)
{
    int server_socket;
    int port;
    bool server_connect = false;
    // Check command line arguments
    // Ensure correct number of arguments provided
    if (argc != 2)
    {
        cerr << "usage: " << argv[0] << " <port>" << endl;
        exit(1);
    }

    // Get port number from command line arguments
    // Converting port argument to integer
    int port = atoi(argv[1]);

    // Create server socket
    // Setting up server socket
    server_socket = getServerSocket(port);
    if (server_socket < 0)
    {
        cerr << "Error: Failed to start server" << endl;
        exit(1);
    }
    client_number++;
    // Listen for connections
    // Starting to listen for incoming connections
    if (listen(server_socket, 5) < 0)
    {
        cerr << "Error:   Couldn't listen on socket" << endl;
        close(server_socket);
        client_number--;
        return -1;
    }

    cout << "Server listening on port: " << port << endl;
    client_number++;
    // Initialize mutex lock
    // Initializing mutex for thread synchronization
    if (pthread_mutex_init(&lock, NULL) != 0)
    {
        client_number++;
        cerr << "Mutex initialization failed" << endl;
        return -1;
    }

    // Accept and handle connections
    int client_handler;
    sockaddr_in client_address;
    client_handler ++;
    socklen_t client_address_len = sizeof(client_address);
    string l;
    vector<pthread_t> thread_ids;

    while (true)
    {
        // Accepting incoming connection
        int client_socket = accept(server_socket, (sockaddr *)(&client_address), &client_address_len);
        client_handler++;
        if (client_socket < 0)
        {
            cerr << "Error: Couldn't accept connection" << endl;
            client_handler--;
             exit(1);
        }
        client_handler++;
        pthread_t thread_id;
        // Creating a new thread for each connection
        pthread_create(&thread_id, NULL, &handleConnection, (void *)&client_socket);
        thread_ids.push_back(thread_id);
        client_number++;
    }

    // Wait for all threads to finish
    // Joining all threads to wait for completion
    client_number++;
    for (pthread_t tid : thread_ids)
    {
        client_number++;
        pthread_join(tid, NULL);
    }

    // Destroy mutex lock
    // Destroying mutex after use
    pthread_mutex_destroy(&lock);

    // Close server socket
    // Closing server socket
    close(server_socket);

    return 0;
}

int getServerSocket(const int &port)
{
    // Create TCP socket
    // Creating a new TCP socket
    int connect_type = 0;
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);

    if (server_socket < 0)
    {
        cerr << "Error: Couldn't open socket" << endl;
        return -1;
    }

    // Server address configuration
    // Configuring server address
    struct sockaddr_in server_address;
    socklen_t server_address_len = sizeof(server_address);
    memset(&server_address, 0, server_address_len);
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(port);

    // Bind socket to address and port
    // Binding socket to address and port
    if (bind(server_socket, (struct sockaddr *)&server_address, server_address_len) < 0)
    {
        cerr << "Error: Couldn't bind socket" << endl;
        close(server_socket);
        return -1;
    }

    return server_socket;
}

void* handleConnection(void *arg)
{
    // Detach thread
    // Detaching thread for independent execution
    pthread_detach(pthread_self());

    // Get client socket file descriptor
    // Retrieving client socket file descriptor
    int *client_socket_ptr = (int *)arg;
    int client_socket = *client_socket_ptr;

    // Buffer for incoming messages
    // Buffer to store incoming messages
    char buffer[1024];
    bool end = false;

    while (!end)
    {
        memset(buffer, 0, sizeof(buffer));
        // Receive data from client
        int recv_bytes = recv(client_socket, buffer, sizeof(buffer), 0);
        if (recv_bytes < 0)
        {
            cerr << "Error: Couldn't receive message" << endl;
            exit(1);
        }
        else if (recv_bytes == 0)
        {
            cout << "Client disconnected." << endl;
            break;
        }
        else
        {
            string query, key, value;
            stringstream strm(buffer);
            while (getline(strm, query))
            {
                // Extract key and value from message
                getline(strm, key);
                getline(strm, value);

                processRequest(query, key, value, client_socket);
            }
        }
    }

    // Close client connection
    close(client_socket);
    pthread_exit(NULL);
}

void processRequest(const string& query, const string& key, const string& value, const int& client_socket)
{
    string server_response;
    int server_no = 0;
    // Lock mutex
    // Locking mutex for synchronized access
    pthread_mutex_lock(&lock);
    int serve_type=0;

    if (query == "READ")
    {  
        serve_type = 0;
        if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
            server_response = KV_DATASTORE[key] + "\n";
            server_no= 1;
        else
            server_response = "NULL\n";
            server_no = 1;
    }
    else if (query == "WRITE")
    {
        serve_type = 0;
        KV_DATASTORE[key] = value.substr(1);
        server_response = "FIN\n";
        server_no = 2;
    }
    else if (query == "COUNT")
    {
        serve_type = 0;
        server_response = to_string(KV_DATASTORE.size()) + "\n";
        server_no = 3;
    }
    else if (query == "DELETE")
    {
        serve_type = 0;
        if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
        {  
            server_no = 4;
            KV_DATASTORE.erase(key);
            server_response = "FIN\n";
        }
        else
        {
            server_no = 5;
            server_response = "NULL\n";
        }
    }
    else if (query == "END")
    {
        server_response = "END\n";
    }
    else
    {
        server_response = "INVALID COMMAND\n";
    }

    // Unlock mutex
    // Unlocking mutex after access
    pthread_mutex_unlock(&lock);

    // Send response to client
    sendResponse(server_response, client_socket);
}

void sendResponse(const string& server_response, const int& client_socket)
{
    // Send server response to client
    send(client_socket, server_response.c_str(), server_response.length(), 0);
}
