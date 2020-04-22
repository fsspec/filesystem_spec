import requests
from ..spec import AbstractFileSystem
from ..utils import infer_storage_options
from .memory import MemoryFile


class GithubFileSystem(AbstractFileSystem):
    """Interface to files in github

    An instance of this class provides the files residing within a remote github
    repository. You may specify a point in the repos history, by SHA, branch
    or tag (default is current master).

    Given that code files tend to be small, and that github does not support
    retrieving partial content, we always fetch whole files.

    When using fsspec.open, allows URIs of the form:

    - "github://path/file", in which case you must specify org, repo and
      may specify sha in the extra args
    - 'github://org:repo@/precip/catalog.yml', where the org and repo are
      part of the URI
    - 'github://org:repo@sha/precip/catalog.yml', where tha sha is also included

    ``sha`` can be the full or abbreviated hex of the commit you want to fetch
    from, or a branch or tag name (so long as it doesn't contain special characters
    like "/", "?", which would have to be HTTP-encoded).
    """

    url = "https://api.github.com/repos/{org}/{repo}/git/trees/{sha}"
    rurl = "https://raw.githubusercontent.com/{org}/{repo}/{sha}/{path}"
    protocol = "github"

    def __init__(self, org, repo, sha="master", **kwargs):
        super().__init__(**kwargs)
        self.org = org
        self.repo = repo
        self.root = sha
        self.ls("")

    @classmethod
    def repos(cls, org_or_user, is_org=True):
        """List repo names for given org or user

        This may become the top level of the FS

        Parameters
        ----------
        org_or_user: str
            Nmae of the github org or user to query
        is_org: bool (default True)
            Whether the name is an organisation (True) or user (False)

        Returns
        -------
        List of string
        """
        r = requests.get(
            "https://api.github.com/{part}/{org}/repos".format(
                part=["users", "orgs"][is_org], org=org_or_user
            )
        )
        r.raise_for_status()
        return [repo["name"] for repo in r.json()]

    def ls(self, path, detail=False, sha=None, **kwargs):
        path = self._strip_protocol(path)
        if path == "":
            sha = self.root
        if sha is None:
            parts = path.rstrip("/").split("/")
            so_far = ""
            sha = self.root
            for part in parts:
                out = self.ls(so_far, True, sha=sha)
                so_far += "/" + part if so_far else part
                out = [o for o in out if o["name"] == so_far][0]
                if out["type"] == "file":
                    if detail:
                        return [out]
                    else:
                        return path
                sha = out["sha"]
        if path not in self.dircache:
            r = requests.get(self.url.format(org=self.org, repo=self.repo, sha=sha))
            if r.status_code == 404:
                raise FileNotFoundError(path)
            r.raise_for_status()
            self.dircache[path] = [
                {
                    "name": path + "/" + f["path"] if path else f["path"],
                    "mode": f["mode"],
                    "type": {"blob": "file", "tree": "directory"}[f["type"]],
                    "size": f.get("size", 0),
                    "sha": f["sha"],
                }
                for f in r.json()["tree"]
            ]
        if detail:
            return self.dircache[path]
        else:
            return sorted([f["name"] for f in self.dircache[path]])

    @classmethod
    def _strip_protocol(cls, path):
        opts = infer_storage_options(path)
        if "username" not in opts:
            return super()._strip_protocol(path)
        return opts["path"].lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(path):
        opts = infer_storage_options(path)
        if "username" not in opts:
            return {}
        out = {"org": opts["username"], "repo": opts["password"]}
        if opts["host"]:
            out["sha"] = opts["host"]
        return out

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        sha=None,
        **kwargs
    ):
        if mode != "rb":
            raise NotImplementedError
        url = self.rurl.format(
            org=self.org, repo=self.repo, path=path, sha=sha or self.root
        )
        r = requests.get(url)
        if r.status_code == 404:
            raise FileNotFoundError(path)
        r.raise_for_status()
        return MemoryFile(None, None, r.content)
