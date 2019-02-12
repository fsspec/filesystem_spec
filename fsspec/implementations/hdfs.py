from pyarrow.hdfs import HadoopFileSystem
from ..spec import AbstractFileSystem


class PyArrowHDFS(AbstractFileSystem):
    """Adapted version of Arrow's HadoopFileSystem

    This is a very simple wrapper over pa.hdfs.HadoopFileSystem, which
    passes on all calls to the underlying class.
    """

    def __init__(self, host="default", port=0, user=None, kerb_ticket=None,
                 driver='libhdfs', extra_conf=None):
        """

        Parameters
        ----------
        host: str
            Hostname, IP or "default" to try to read from Hadoop config
        port: int
            Port to connect on, or default from Hadoop config if 0
        user: str or None
            If given, connect as this username
        kerb_ticket: str or None
            If given, use this ticket for authentication
        driver: 'libhdfs' or 'libhdfs3'
            Binary driver; libhdfs if the JNI library and default
        extra_conf: None or dict
            Passed on to HadoopFileSystem
        """
        self.driver = HadoopFileSystem(host=host, port=port, user=user,
                                       kerb_ticket=kerb_ticket, driver=driver,
                                       extra_conf=extra_conf)

    def _open(self, path, mode='rb', block_size=None, autocommit=True,
              **kwargs):
        """

        Parameters
        ----------
        path: str
            Location of file; should start with '/'
        mode: str
        block_size: int
            Hadoop block size, e.g., 2**26
        autocommit: True
            Transactions are not yet implemented for HDFS; errors if not True
        kwargs: dict or None
            Hadoop config parameters

        Returns
        -------
        arrow HdfsFile file-like instance
        """
        if not autocommit:
            raise NotImplementedError
        return self.driver.open(path, mode, block_size, **kwargs)

    def __getattr__(self, item):
        if item in ['chmod', 'chown',
                    'df', 'disk_usage', 'download', 'driver', 'exists',
                    'extra_conf', 'get_capacity', 'get_space_used', 'host',
                    'info', 'is_open', 'isdir', 'isfile', 'kerb_ticket',
                    'ls', 'mkdir', 'mv', 'port',
                    'read_parquet', 'rm', 'stat', 'upload',
                    'user', 'walk']:
            return getattr(self.driver, item)

