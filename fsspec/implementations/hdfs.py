from pyarrow.hdfs import HadoopFileSystem
from ..spec import AbstractFileSystem


class PyArrowHDFS(AbstractFileSystem):

    def __init__(self, host="default", port=0, user=None, kerb_ticket=None,
                 driver='libhdfs', extra_conf=None):

        self.driver = HadoopFileSystem(host=host, port=port, user=user,
                                       kerb_ticket=kerb_ticket, driver=driver,
                                       extra_conf=extra_conf)

    def _open(self, path, mode='rb', block_size=None, autocommit=True,
              **kwargs):
        """
        kwargs: replication (int), default_block_size (int)
        """
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

