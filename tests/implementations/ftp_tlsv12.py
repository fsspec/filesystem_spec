import os
import ssl

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer


def ftp():
    """Script to run FTP server that accepts TLS v1.2 implicitly"""
    # Set up FTP server parameters
    FTP_HOST = "localhost"
    FTP_PORT = 2122
    FTP_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

    # Instantiate a dummy authorizer
    authorizer = DummyAuthorizer()
    authorizer.add_user(
        "user",
        "pass",
        FTP_DIRECTORY,
        "elradfmwMT",
    )
    authorizer.add_anonymous(FTP_DIRECTORY)

    # Instantiate FTPHandler for implicit TLS
    handler = FTPHandler
    handler.authorizer = authorizer

    # Instantiate FTP server
    server = FTPServer((FTP_HOST, FTP_PORT), handler)

    # Wrap socket for implicit TLS
    certfile = os.path.join(os.path.dirname(__file__), "keycert.pem")
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.load_cert_chain(certfile)
    server.socket = context.wrap_socket(server.socket, server_side=True)

    server.serve_forever()


if __name__ == "__main__":
    ftp()
