import os

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import TLS_FTPHandler
from pyftpdlib.servers import FTPServer


def ftp():
    # Set up FTP server parameters
    FTP_HOST = "0.0.0.0"
    FTP_PORT = 2121  # Choose a free port for the FTP server
    FTP_DIRECTORY = os.path.dirname(__file__)
    print(FTP_DIRECTORY)

    # Instantiate a dummy authorizer
    authorizer = DummyAuthorizer()
    authorizer.add_user(
        "user",
        "pass",
        FTP_DIRECTORY,
        "elradfmwMT",
    )
    authorizer.add_anonymous(FTP_DIRECTORY)

    # Instantiate TLS_FTPHandler with required parameters
    handler = TLS_FTPHandler
    handler.certfile = os.path.join(os.path.dirname(__file__), "keycert.pem")
    handler.authorizer = authorizer

    # Instantiate FTP server with TLS handler and authorizer
    server = FTPServer((FTP_HOST, FTP_PORT), handler)
    server.authorizer = authorizer

    server.serve_forever()


if __name__ == "__main__":
    ftp()
