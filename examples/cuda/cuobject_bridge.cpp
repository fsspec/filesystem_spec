// Minimal C ABI for the CUDA Toolkit's C++ cuObject client.
// Build instructions and control callback contract: docs/source/gpu.rst.
#include <memory>
#include <cuobjclient.h>
#include <exception>

using Control = ssize_t (*)(void *, const char *, size_t, size_t, size_t);
struct Request {
    Control control;
    void *context;
    size_t object_offset;
};

static ssize_t get(const void *handle, char *, size_t size, loff_t offset,
                   const cufileRDMAInfo_t *rdma) {
    auto *request = static_cast<Request *>(cuObjClient::getCtx(handle));
    if (!rdma || !rdma->desc_str || rdma->desc_len <= 0) return -1;
    return request->control(request->context, rdma->desc_str, rdma->desc_len,
                            size, request->object_offset + offset);
}

extern "C" ssize_t fsspec_cuobject_read(void *buffer, size_t size, size_t offset,
                                       Control control, void *context) noexcept {
    try {
        CUObjOps_t ops{};
        ops.get = get;
        cuObjClient client(ops);
        if (!client.isConnected()) return -1;
        Request request{control, context, offset};
        return client.cuObjGet(&request, buffer, size);
    } catch (const std::exception &) {
        return -1;
    } catch (...) {
        return -1;
    }
}
