"""Optional C ABI bridge to cuObject; requires a storage-specific control callback."""

import ctypes


def readinto(path, out, offset, *, library, control):
    """Read using cuObject and a signed S3/HTTP control-plane request.

    ``control(path, token, size, offset) -> int`` must send the RDMA token,
    verify the RDMA reply and return the completed byte count. It must raise
    if RDMA is unavailable; a response body cannot fill device memory.
    The native call blocks until RDMA completes. No pinned fallback is used.
    """
    bridge = ctypes.CDLL(library)
    callback_type = ctypes.CFUNCTYPE(
        ctypes.c_ssize_t,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.c_size_t,
    )
    errors = []

    def callback(context, token, token_size, size, object_offset):
        try:
            return control(
                path, ctypes.string_at(token, token_size), size, object_offset
            )
        except BaseException as exc:
            errors.append(exc)
            return -1

    callback_ptr = callback_type(callback)
    read = bridge.fsspec_cuobject_read
    read.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
        callback_type,
        ctypes.c_void_p,
    ]
    read.restype = ctypes.c_ssize_t
    count = read(int(out.handle), out.size, offset, callback_ptr, None)
    if errors:
        raise errors[0]
    if count != out.size:
        raise OSError(f"cuObject read failed: expected {out.size}, got {count}")
    return count
