import socket
import threading
import time


# Global variables
server_ip = "vayu.iitd.ac.in"
server_port = 9801
max_lines = 1000
lines_received = [0]*max_lines
full_text = [""]*max_lines
lines_count = 0
lines_lock = threading.Lock()

# Function to connect to the server and download lines
def download_lines(client_socket):
    global lines_count
    start_time = time.time()
    prev = -1
    worthy = 0
    while lines_count <= max_lines:
        if lines_count <max_lines:
            client_socket.send("SENDLINE\n".encode())
        else:
            lines_count +=1
        line = client_socket.recv(1024).decode().strip()
        curr_time = time.time()
        line_list = line.split('\n')
        if (len(line_list) >=2):
            for i in range(0,len(line_list)):
                curr_element = line_list[i]
                if (curr_element.isdigit()):
                    line_num = int(curr_element)
                    if lines_received[line_num-1] == 0:
                        prev = line_num-1
                        full_text[prev] = line_list[i+1]
                        lines_count = lines_count +1
                        lines_received[line_num -1] = 1
                        worthy = 1
                        print(lines_count)
                        print(curr_time - start_time)
                elif worthy == 1:
                    full_text[prev] = full_text[prev] + curr_element
                    worthy = 0     
        elif (line_list != ['-1'] and worthy == 1):
            text = line_list[0]
            full_text[prev] = full_text[prev] + text
            worthy = 0
        else:
            worthy = 0
    client_socket.send("SUBMIT\n".encode())
    client_socket.send("cs5210618@iitd.ac.in\n".encode())
    client_socket.send("1000\n".encode())
    for i in range(0,1000):
        client_socket.send((str(i+1)+"\n").encode())
        client_socket.send((full_text[i]+"\n").encode())
    output = client_socket.recv(1024).decode().strip()
    print(output)
    print(curr_time -start_time)
    
def main():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_ip, server_port))
    download_thread = threading.Thread(target=download_lines, args=(client_socket,))
    download_thread.start()
    download_thread.join()
    client_socket.close()

if __name__ == "__main__":
    main()