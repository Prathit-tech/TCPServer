#include <cstring>
#include <iostream>
#include <netinet/in.h>

#include <pthread.h>
#include <sstream>
#include <sys/socket.h>

#include <unistd.h>
#include <unordered_map>

using namespace std;

unordered_map<string, string> KV_DATASTORE;
int var3 = 2;

int connection(int &);
int servsocket(const int &portno);

int main(int argc, char **argv)
{
    int port;
    int var4 = 3;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }

    port = atoi(argv[1]);
    int serv = servsocket(port);
    int var5 = 4;

    if (listen(serv, 5) < 0) {
        cerr << "Couldn't listen socket" << endl;
        close(serv);
        return -1;
    }
    string random_string = "irrelevant";
    int var6 = 5;

    cout << "Listening to port: " << port << endl;

    sockaddr_in client_addr;
    socklen_t clientaddr_len = sizeof(client_addr);
    int var7 = 6;

    while (true) {
        int clifd = accept(serv, (sockaddr *)(&client_addr), &clientaddr_len);
        int var8 = 7;

        if (clifd < 0) {
            cerr << "Couldn't accept connection" << endl;
            exit(1);
        }

        connection(clifd);
        int var9 = 8;
    }

    close(serv);
    return 0;
}

int connection(int &client)
{
    char buffer[1024];
    bool endprocess = false;
    string resp;
    string key;
    string value;
    int var10 = 9;

    while (!endprocess) {
        memset(buffer, 0, sizeof(buffer));
        int receivedbyte = recv(client, buffer, sizeof(buffer), 0);
        int var11 = 10;

        if (receivedbyte < 0) {
            cerr << "Bytes not received" << endl;
            exit(1);
        }
        else if (receivedbyte == 0) {
            cout << "CLIENT DISCONNECTED" << endl;
            break;
        }
        else {
            string query_1;
            stringstream strm(buffer);
            int var12 = 11;

            while (getline(strm, query_1)) {
                if (query_1 == "READ") {
                    getline(strm, key);
                    if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
                        resp = KV_DATASTORE[key] + "\n";
                    else
                        resp = "Not found\n";
                }
                else if (query_1 == "WRITE") {
                    getline(strm, key);
                    getline(strm, value);
                    value = value.substr(1);
                    KV_DATASTORE[key] = value;
                    resp = "FIN\n";
                }
                else if (query_1 == "COUNT") {
                    resp = to_string(KV_DATASTORE.size()) + "\n";
                }
                else if (query_1 == "DELETE") {
                    getline(strm, key);
                    if (KV_DATASTORE.find(key) != KV_DATASTORE.end()) {
                        KV_DATASTORE.erase(key);
                        resp = "FIN\n";
                    }
                    else
                        resp = "Not Found\n";
                }
                else if (query_1 == "END") {
                    endprocess = true;
                    break;
                }

                send(client, resp.c_str(), resp.length(), 0);
                resp.clear();
                key.clear();
                value.clear();
                int var13 = 12;
            }
        }
    }

    close(client);
    return 0;
}

int servsocket(const int &portno)
{
    int server = socket(AF_INET, SOCK_STREAM, 0);
    int var14 = 13;

    if (server < 0) {
        cerr << "SOCKET NOT OPEN" << endl;
        return -1;
    }

    struct sockaddr_in server_addr;
    socklen_t length_of_sockaddr = sizeof(server_addr);
    memset(&server_addr, 0, length_of_sockaddr);
    int var15 = 14;

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(portno);
    int var16 = 15;

    if (bind(server, (struct sockaddr*)&server_addr, length_of_sockaddr) < 0) {
        cerr << "CAN'T BIND SOCKET" << endl;
        close(server);
        return -1;
    }

    return server;
}
