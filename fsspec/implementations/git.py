import pygit2
from fsspec.spec import AbstractFileSystem
from .memory import MemoryFile
import os


class GitFileSystem(AbstractFileSystem):

    root_marker = ""

    def __init__(self, path=None, ref=None, **kwargs):
        super().__init__(**kwargs)
        self.repo = pygit2.Repository(path or os.getcwd())
        self.ref = ref or 'master'

    @classmethod
    def _strip_protocol(cls, path):
        return super()._strip_protocol(path).lstrip('/')

    def _path_to_object(self, path, ref):
        comm, ref = self.repo.resolve_refish(ref or self.ref)
        parts = path.split('/')
        tree = comm.tree
        for part in parts:
            if part and isinstance(tree, pygit2.Tree):
                tree = tree[part]
        return tree

    def ls(self, path, detail=True, ref=None, **kwargs):
        path = self._strip_protocol(path)
        tree = self._path_to_object(path, ref)
        if isinstance(tree, pygit2.Tree):
            out = []
            for obj in tree:
                if isinstance(obj, pygit2.Tree):
                    out.append({'type': 'directory',
                                'name': '/'.join([path, obj.name]),
                                'hex': obj.hex,
                                'mode': "%o" % obj.filemode,
                                'size': 0})
                else:
                    out.append({'type': 'file',
                                'name': '/'.join([path, obj.name]),
                                'hex': obj.hex,
                                'mode': "%o" % obj.filemode,
                                'size': obj.size})
        else:
            out = [tree]
        if detail:
            return out
        return [o['name'] for o in out]

    def ukey(self, path, ref=None):
        return self.info(path, ref=ref)['hex']

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        ref=None,
        **kwargs
    ):
        obj = self._path_to_object(path, ref)
        return MemoryFile(data=obj.data)
