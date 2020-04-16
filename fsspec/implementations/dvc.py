import os
from fsspec.spec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
import dvc.repo
lfs = LocalFileSystem()


class DVCFileSystem(AbstractFileSystem):

    root_marker = ""

    def __init__(self, path=None, **kwargs):
        super().__init__(**kwargs)
        self.repo = dvc.repo.Repo(path)
        self.path = self.repo.find_root()

    @classmethod
    def _strip_protocol(cls, path):
        return super()._strip_protocol(path).lstrip('/')

    def ls(self, path, detail=False, **kwargs):
        path = self._strip_protocol(path)
        allfiles = self.repo.tree.walk(os.path.join(self.repo.root_dir, path))
        dirname, dirs, files = next(allfiles)
        out = [os.path.join(path, f) for f in dirs + files]
        details = []

        for f in out:
            full = os.path.join(self.repo.root_dir, f)
            file_info = lfs.info(full)
            if lfs.isdir(full):
                details.append(file_info)
            else:
                try:
                    extra = self.repo.find_out_by_relpath(full).dumpd()
                except dvc.exceptions.OutputNotFoundError:
                    continue
                details.append(dict(**extra, **file_info))
            details[-1]['name'] = f
        if detail:
            return details
        return [d['name'] for d in details]

    def ukey(self, path):
        return self.info(path)['md5']

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        # returns a context file object (i.e., needs to be used with ``with``
        path = self._strip_protocol(path)
        return self.repo.open_by_relpath(path)
