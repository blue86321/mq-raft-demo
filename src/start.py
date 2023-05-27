import configparser
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

class RequestHandler(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self._set_response()
        self.wfile.write("Hello, distributed system!".encode('utf-8'))

def start_server(ip, port):
    server_address = (ip, port)
    httpd = HTTPServer(server_address, RequestHandler)
    print(f"Starting server on {ip}:{port}...")
    httpd.serve_forever()

def initialize_system():
    config = configparser.ConfigParser()
    config.read('config.ini')

    num_nodes = int(config['General']['num_nodes'])
    node_ips = config['General']['node_ips'].split(',')
    node_ports = config['General']['node_ports'].split(',')

    if num_nodes != len(node_ips) or num_nodes != len(node_ports):
        print("Error: Configuration mismatch!")
        return

    threads = []
    for i in range(num_nodes):
        t = threading.Thread(target=start_server, args=(node_ips[i], int(node_ports[i])))
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    initialize_system()
