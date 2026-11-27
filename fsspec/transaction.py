import logging
from collections import deque

logger = logging.getLogger("fsspec")


class Transaction:
    """Filesystem transaction write context

    Gathers files for deferred commit or discard, so that several write
    operations can be finalized semi-atomically. This works by having this
    instance as the ``.transaction`` attribute of the given filesystem
    """

    def __init__(self, fs, **kwargs):
        """
        Parameters
        ----------
        fs: FileSystem instance
        """
        self.fs = fs
        self.files = deque()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End transaction and commit, if exit is not due to exception"""
        # only commit if there was no exception
        self.complete(commit=exc_type is None)
        if self.fs:
            self.fs._intrans = False
            self.fs._transaction = None
            self.fs = None

    def start(self):
        """Start a transaction on this FileSystem"""
        self.files = deque()  # clean up after previous failed completions
        self.fs._intrans = True

    def complete(self, commit=True):
        """Finish transaction: commit or discard all deferred files"""
        try:
            _finish_files(self.files, commit)
        finally:
            # A failed commit or discard must still end the transaction.
            # Leaving _intrans set would defer every later write on this
            # filesystem into a temporary file that nothing ever commits,
            # and instances are cached, so that would persist process-wide.
            self.fs._intrans = False
            self.fs._transaction = None
            self.fs = None


def _finish_files(files, commit):
    current = None
    try:
        while files:
            current = files.popleft()
            if commit:
                current.commit()
            else:
                current.discard()
            current = None
    finally:
        if current is not None:
            files.appendleft(current)
        while files:
            try:
                files.popleft().discard()
            except Exception:
                logger.debug("Discarding deferred file failed", exc_info=True)


class FileActor:
    def __init__(self):
        self.files = deque()

    def commit(self):
        _finish_files(self.files, commit=True)

    def discard(self):
        _finish_files(self.files, commit=False)

    def append(self, f):
        self.files.append(f)


class DaskTransaction(Transaction):
    def __init__(self, fs):
        """
        Parameters
        ----------
        fs: FileSystem instance
        """
        import distributed

        super().__init__(fs)
        client = distributed.default_client()
        self.files = client.submit(FileActor, actor=True).result()

    def start(self):
        # The worker actor owns the deferred files for this transaction.
        self.fs._intrans = True

    def complete(self, commit=True):
        """Finish transaction: commit or discard all deferred files"""
        try:
            if commit:
                self.files.commit().result()
            else:
                self.files.discard().result()
        finally:
            self.fs._intrans = False
            self.fs._transaction = None
            self.fs = None
