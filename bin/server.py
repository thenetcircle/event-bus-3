from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Print request headers
        print("==========")
        print("Headers:\n", self.headers)

        # Determine the length of the data
        content_length = int(self.headers["Content-Length"])

        # Read and print the body of the POST request
        post_data = self.rfile.read(content_length)
        print("Body:\n", post_data.decode("utf-8"))
        print("==========\n\n\n")

        # Send response
        self.send_response(200)
        self.end_headers()
        response = b"This is the POST response."
        self.wfile.write(response)


def run(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler, port=8002):
    server_address = ("", port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting httpd server on port {port}")
    httpd.serve_forever()


if __name__ == "__main__":
    run(port=8002)
