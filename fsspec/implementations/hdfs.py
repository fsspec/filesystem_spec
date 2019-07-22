from ..spec import AbstractFileSystem


class PyArrowHDFS(AbstractFileSystem):
    """Adapted version of Arrow's HadoopFileSystem

    This is a very simple wrapper over pa.hdfs.HadoopFileSystem, which
    passes on all calls to the underlying class.
    """

    def __init__(self, host="default", port=0, user=None, kerb_ticket=None,
                 driver='libhdfs', extra_conf=None, **kwargs):
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
        from pyarrow.hdfs import HadoopFileSystem
        AbstractFileSystem.__init__(self, **kwargs)
        self.pars = (host, port, user, kerb_ticket, driver, extra_conf)
        self.pahdfs = HadoopFileSystem(host=host, port=port, user=user,
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
        return self.pahdfs.open(path, mode, block_size, **kwargs)

    def __reduce_ex__(self, protocol):
        return PyArrowHDFS, self.pars

    def ls(self, path, detail=True):
        out = self.pahdfs.ls(path, detail)
        if detail:
            for p in out:
                p['type'] = p['kind']
        return out

    def __getattribute__(self, item):
        if item in ['_open', '__init__', '__getattribute__', '__reduce_ex__',
                    'open', 'ls']:
            # all the methods defined in this class. Note `open` here, since
            # it calls `_open`, but is actually in superclass
            return lambda *args, **kw: getattr(PyArrowHDFS, item)(
                self, *args, **kw
            )
        if item == '__class__':
            return PyArrowHDFS
        d = object.__getattribute__(self, '__dict__')
        pahdfs = d.get('pahdfs', None)  # fs is not immediately defined
        if pahdfs is not None and item in [
            'chmod', 'chown', 'user',
            'df', 'disk_usage', 'download', 'driver', 'exists',
            'extra_conf', 'get_capacity', 'get_space_used', 'host',
            'is_open', 'kerb_ticket',
            'mkdir', 'mv', 'port', 'get_capacity',
            'get_space_used', 'df', 'chmod', 'chown', 'disk_usage',
            'download', 'upload',
            'read_parquet', 'rm', 'stat', 'upload',
        ]:
            return getattr(pahdfs, item)
        else:
            # attributes of the superclass, while target is being set up
            return super().__getattribute__(item)

