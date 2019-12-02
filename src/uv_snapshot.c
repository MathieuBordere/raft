#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "array.h"
#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "heap.h"
#include "logging.h"
#include "uv.h"
#include "uv_encoding.h"
#include "uv_error.h"
#include "uv_os.h"

/* Arbitrary maximum configuration size. Should be practically be enough */
#define META_MAX_CONFIGURATION_SIZE 1024 * 1024

/* Check if the given filename matches the one of a snapshot metadata filename
 * (snapshot-xxx-yyy-zzz.meta), and fill the given info structure if so.
 *
 * Return true if the filename matched, false otherwise. */
static bool uvSnapshotInfoMatch(const char *filename, struct uvSnapshotInfo *info)
{
    int consumed = 0;
    int matched;
    size_t filename_len = strnlen(filename, UV__FILENAME_LEN + 1);

    if (filename_len > UV__FILENAME_LEN) {
        return false;
    }

    matched = sscanf(filename, UV__SNAPSHOT_META_TEMPLATE "%n", &info->term,
                     &info->index, &info->timestamp, &consumed);
    if (matched != 3 || consumed != (int)filename_len) {
        return false;
    }

    strcpy(info->filename, filename);
    return true;
}

/* Render the filename of the data file of a snapshot */
static void uvSnapshotFilenameOf(struct uvSnapshotInfo *info, char *filename)
{
    size_t len = strlen(info->filename) - strlen(".meta");
    assert(len < UV__FILENAME_LEN);
    strncpy(filename, info->filename, len);
    filename[len] = 0;
}

int uvSnapshotInfoAppendIfMatch(struct uv *uv,
                                const char *filename,
                                struct uvSnapshotInfo *infos[],
                                size_t *n_infos,
                                bool *appended)
{
    struct uvSnapshotInfo info;
    bool matched;
    char snapshot_filename[UV__FILENAME_LEN];
    bool exists;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    int rv;

    /* Check if it's a snapshot metadata filename */
    matched = uvSnapshotInfoMatch(filename, &info);
    if (!matched) {
        *appended = false;
        return 0;
    }

    /* Check if there's actually a snapshot file for this snapshot metadata. If
     * there's none, it means that we aborted before finishing the snapshot, so
     * let's remove the metadata file. */
    uvSnapshotFilenameOf(&info, snapshot_filename);
    rv = UvFsFileExists(uv->dir, snapshot_filename, &exists, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "stat %s: %s", snapshot_filename, errmsg);
        rv = RAFT_IOERR;
        return rv;
    }
    if (!exists) {
        UvFsRemoveFile(uv->dir, filename, errmsg); /* Ignore errors */
        *appended = false;
        return 0;
    }

    ARRAY__APPEND(struct uvSnapshotInfo, info, infos, n_infos, rv);
    if (rv == -1) {
        return RAFT_NOMEM;
    }

    *appended = true;

    return 0;
}

/* Compare two snapshots to decide which one is more recent. */
static int uvSnapshotCompare(const void *p1, const void *p2)
{
    struct uvSnapshotInfo *s1 = (struct uvSnapshotInfo *)p1;
    struct uvSnapshotInfo *s2 = (struct uvSnapshotInfo *)p2;

    /* If terms are different, the snaphot with the highest term is the most
     * recent. */
    if (s1->term != s2->term) {
        return s1->term < s2->term ? -1 : 1;
    }

    /* If the term are identical and the index differ, the snapshot with the
     * highest index is the most recent */
    if (s1->index != s2->index) {
        return s1->index < s2->index ? -1 : 1;
    }

    /* If term and index are identical, compare the timestamp. */
    return s1->timestamp < s2->timestamp ? -1 : 1;
}

void uvSnapshotSort(struct uvSnapshotInfo *infos, size_t n_infos)
{
    qsort(infos, n_infos, sizeof *infos, uvSnapshotCompare);
}

/* Parse the metadata file of a snapshot and populate the given snapshot object
 * accordingly. */
static int uvSnapshotLoadMeta(struct uv *uv,
                              struct uvSnapshotInfo *info,
                              struct raft_snapshot *snapshot)
{
    uint64_t header[1 + /* Format version */
                    1 + /* CRC checksum */
                    1 + /* Configuration index */
                    1 /* Configuration length */];
    struct raft_buffer buf;
    unsigned format;
    unsigned crc1;
    unsigned crc2;
    uv_file fd;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    int rv;

    snapshot->term = info->term;
    snapshot->index = info->index;

    rv = UvFsOpenFileForReading(uv->dir, info->filename, &fd, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "open %s: %s", info->filename, errmsg);
        rv = RAFT_IOERR;
        goto err;
    }
    buf.base = header;
    buf.len = sizeof header;
    rv = UvFsReadInto(fd, &buf, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "read %s: %s", info->filename, errmsg);
        rv = RAFT_IOERR;
        goto err_after_open;
    }

    format = byteFlip64(header[0]);
    if (format != UV__DISK_FORMAT) {
        Tracef(uv->tracer, "load %s: unsupported format %lu", info->filename,
               format);
        rv = RAFT_MALFORMED;
        goto err_after_open;
    }

    crc1 = byteFlip64(header[1]);

    snapshot->configuration_index = byteFlip64(header[2]);
    buf.len = byteFlip64(header[3]);
    if (buf.len > META_MAX_CONFIGURATION_SIZE) {
        Tracef(uv->tracer, "load %s: configuration data too big (%ld)",
               info->filename, buf.len);
        rv = RAFT_CORRUPT;
        goto err_after_open;
    }
    if (buf.len == 0) {
        Tracef(uv->tracer, "load %s: no configuration data", info->filename,
               buf.len);
        rv = RAFT_CORRUPT;
        goto err_after_open;
    }
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_open;
    }

    rv = UvFsReadInto(fd, &buf, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "read %s: %s", info->filename, errmsg);
        rv = RAFT_IOERR;
        goto err_after_buf_malloc;
    }

    crc2 = byteCrc32(header + 2, sizeof header - sizeof(uint64_t) * 2, 0);
    crc2 = byteCrc32(buf.base, buf.len, crc2);

    if (crc1 != crc2) {
        Tracef(uv->tracer, "read %s: checksum mismatch", info->filename);
        rv = RAFT_CORRUPT;
        goto err_after_open;
    }

    raft_configuration_init(&snapshot->configuration);
    rv = configurationDecode(&buf, &snapshot->configuration);
    if (rv != 0) {
        goto err_after_buf_malloc;
    }

    raft_free(buf.base);
    UvOsClose(fd);

    return 0;

err_after_buf_malloc:
    raft_free(buf.base);

err_after_open:
    close(fd);

err:
    assert(rv != 0);
    return rv;
}

/* Load the snapshot data file. */
static int uvSnapshotLoadData(struct uv *uv,
                              struct uvSnapshotInfo *info,
                              struct raft_snapshot *snapshot)
{
    char filename[UV__FILENAME_LEN];
    struct raft_buffer buf;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    int rv;

    uvSnapshotFilenameOf(info, filename);

    rv = UvFsReadFile(uv->dir, filename, &buf, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "stat %s: %s", filename, errmsg);
        goto err;
    }

    snapshot->bufs = raft_malloc(sizeof *snapshot->bufs);
    snapshot->n_bufs = 1;
    if (snapshot->bufs == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_read_file;
    }

    snapshot->bufs[0] = buf;

    return 0;

err_after_read_file:
    raft_free(buf.base);
err:
    assert(rv != 0);
    return rv;
}

int uvSnapshotLoad(struct uv *uv,
                   struct uvSnapshotInfo *meta,
                   struct raft_snapshot *snapshot)
{
    int rv;
    rv = uvSnapshotLoadMeta(uv, meta, snapshot);
    if (rv != 0) {
        return rv;
    }
    rv = uvSnapshotLoadData(uv, meta, snapshot);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

struct put
{
    struct uv *uv;
    size_t trailing;
    struct raft_io_snapshot_put *req;
    const struct raft_snapshot *snapshot;
    struct
    {
        unsigned long long timestamp;
        uint64_t header[4];         /* Format, CRC, configuration index/len */
        struct raft_buffer bufs[2]; /* Preamble and configuration */
    } meta;
    int status;
    queue queue;
};

struct get
{
    struct uv *uv;
    struct raft_io_snapshot_get *req;
    struct raft_snapshot *snapshot;
    struct uv_work_s work;
    int status;
    queue queue;
};

/* Remove all segmens and snapshots that are not needed anymore. */
static int removeOldSegmentsAndSnapshots(struct uv *uv,
                                         raft_index last_index,
                                         size_t trailing)
{
    struct uvSnapshotInfo *snapshots;
    struct uvSegmentInfo *segments;
    size_t n_snapshots;
    size_t n_segments;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    int rv = 0;

    rv = uvList(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        goto out;
    }

    rv = uvSnapshotKeepLastTwo(uv, snapshots, n_snapshots);
    if (rv != 0) {
        goto out;
    }

    if (segments != NULL) {
        size_t deleted;
        rv = uvSegmentKeepTrailing(uv, segments, n_segments, last_index,
                                   trailing, &deleted);
        if (rv != 0) {
            goto out;
        }
    }

    rv = UvFsSyncDir(uv->dir, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "sync %s: %s", uv->dir, errmsg);
    }

out:
    if (snapshots != NULL) {
        raft_free(snapshots);
    }
    if (segments != NULL) {
        raft_free(segments);
    }

    return rv;
}

int uvSnapshotKeepLastTwo(struct uv *uv,
                          struct uvSnapshotInfo *snapshots,
                          size_t n)
{
    size_t i;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    int rv;

    /* Leave at least two snapshots, for safety. */
    if (n <= 2) {
        return 0;
    }

    for (i = 0; i < n - 2; i++) {
        struct uvSnapshotInfo *s = &snapshots[i];
        char filename[UV__FILENAME_LEN];
        rv = UvFsRemoveFile(uv->dir, s->filename, errmsg);
        if (rv != 0) {
            Tracef(uv->tracer, "unlink %s: %s", s->filename, errmsg);
            return RAFT_IOERR;
        }
        uvSnapshotFilenameOf(s, filename);
        rv = UvFsRemoveFile(uv->dir, filename, errmsg);
        if (rv != 0) {
            Tracef(uv->tracer, "unlink %s: %s", filename, errmsg);
            return RAFT_IOERR;
        }
    }

    return 0;
}

static void uvSnapshotPutWorkCb(uv_work_t *work)
{
    struct put *put = work->data;
    struct uv *uv = put->uv;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    char filename[UV__FILENAME_LEN];
    int rv;

    sprintf(filename, UV__SNAPSHOT_META_TEMPLATE, put->snapshot->term,
            put->snapshot->index, put->meta.timestamp);

    rv = UvFsMakeFile(uv->dir, filename, put->meta.bufs, 2, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "write %s: %s", filename, errmsg);
        put->status = RAFT_IOERR;
        return;
    }

    sprintf(filename, UV__SNAPSHOT_TEMPLATE, put->snapshot->term,
            put->snapshot->index, put->meta.timestamp);

    rv = UvFsMakeFile(uv->dir, filename, put->snapshot->bufs,
                      put->snapshot->n_bufs, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "write %s: %s", filename, errmsg);
        put->status = RAFT_IOERR;
        return;
    }

    rv = UvFsSyncDir(uv->dir, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "sync %s: %s", uv->dir, errmsg);
        put->status = RAFT_IOERR;
        return;
    }

    rv = removeOldSegmentsAndSnapshots(uv, put->snapshot->index, put->trailing);
    if (rv != 0) {
        put->status = rv;
        return;
    }

    put->status = 0;

    return;
}

static void uvSnapshotPutAfterWorkCb(uv_work_t *work, int status)
{
    struct put *put = work->data;
    struct raft_io_snapshot_put *req = put->req;
    int put_status = put->status;
    struct uv *uv = put->uv;
    assert(status == 0);
    QUEUE_REMOVE(&put->queue);
    uv->snapshot_put_work.data = NULL;
    HeapFree(put->meta.bufs[1].base);
    HeapFree(put);
    req->cb(req, put_status);
    uvMaybeFireCloseCb(uv);
}

/* Process pending put requests. */
void uvSnapshotMaybeProcessRequests(struct uv *uv)
{
    struct put *put;
    queue *head;
    int rv;

    /* If there aren't pending snapshot put requests, there's nothing to do. */
    if (QUEUE_IS_EMPTY(&uv->snapshot_put_reqs)) {
        return;
    }
    /* If we're already writing a snapshot, let's wait. */
    if (uv->snapshot_put_work.data != NULL) {
        return;
    }
    /* If there's a pending truncate request, let's wait. Typically the truncate
     * request is initiated by the InstallSnapshot RPC handler. */
    if (uv->barrier != NULL) {
        return;
    }

    /* Get the head of the queue */
    head = QUEUE_HEAD(&uv->snapshot_put_reqs);
    put = QUEUE_DATA(head, struct put, queue);

    /* Detect if we're being run just after a truncate request in order to
     * restore a snaphost, in that case we want to adjust the finalize last
     * index accordingly.
     *
     * TODO: this doesn't work in all cases. Reason about exact sequence of
     * events, make logic more elegant and robust.  */
    if (uv->finalize_last_index == 0) {
        uv->finalize_last_index = put->snapshot->index;
    }

    uv->snapshot_put_work.data = put;
    rv = uv_queue_work(uv->loop, &uv->snapshot_put_work, uvSnapshotPutWorkCb,
                       uvSnapshotPutAfterWorkCb);
    if (rv != 0) {
        Tracef(uv->tracer, "store snapshot %lld: %s", put->snapshot->index,
               uv_strerror(rv));
        uv->errored = true;
    }
}

int UvSnapshotPut(struct raft_io *io,
                  unsigned trailing,
                  struct raft_io_snapshot_put *req,
                  const struct raft_snapshot *snapshot,
                  raft_io_snapshot_put_cb cb)
{
    struct uv *uv;
    struct put *put;
    void *cursor;
    unsigned crc;
    int rv;

    assert(trailing > 0);

    uv = io->impl;
    assert(!uv->closing);

    Tracef(uv->tracer, "put snapshot at %lld, keeping %d", snapshot->index,
           trailing);

    put = HeapMalloc(sizeof *put);
    if (put == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    put->uv = uv;
    put->req = req;
    put->snapshot = snapshot;
    put->meta.timestamp = uv_now(uv->loop);
    put->trailing = trailing;

    req->cb = cb;

    /* Prepare the buffers for the metadata file. */
    put->meta.bufs[0].base = put->meta.header;
    put->meta.bufs[0].len = sizeof put->meta.header;

    rv = configurationEncode(&snapshot->configuration, &put->meta.bufs[1]);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    /* If the next append index is set to 1, it means that we're restoring a
     * snapshot after having trucated the log. Set the next append index to the
     * snapshot's last index + 1. */
    if (uv->append_next_index == 1) {
        uv->append_next_index = snapshot->index + 1;
        /* We expect that a new prepared segment has just been requested, we
         * need to update its first index too.
         *
         * TODO: this should be cleaned up. */
        uvAppendFixPreparedSegmentFirstIndex(uv);
    }

    cursor = put->meta.header;
    bytePut64(&cursor, UV__DISK_FORMAT);
    bytePut64(&cursor, 0);
    bytePut64(&cursor, snapshot->configuration_index);
    bytePut64(&cursor, put->meta.bufs[1].len);

    crc = byteCrc32(&put->meta.header[2], sizeof(uint64_t) * 2, 0);
    crc = byteCrc32(put->meta.bufs[1].base, put->meta.bufs[1].len, crc);

    cursor = &put->meta.header[1];
    bytePut64(&cursor, crc);

    QUEUE_PUSH(&uv->snapshot_put_reqs, &put->queue);
    uvSnapshotMaybeProcessRequests(uv);

    return 0;

err_after_req_alloc:
    HeapFree(put);
err:
    assert(rv != 0);
    return rv;
}

static void uvSnapshotGetWorkCb(uv_work_t *work)
{
    struct get *r = work->data;
    struct uv *uv = r->uv;
    struct uvSnapshotInfo *snapshots;
    size_t n_snapshots;
    struct uvSegmentInfo *segments;
    size_t n_segments;
    int rv;
    r->status = 0;
    rv = uvList(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        r->status = rv;
        goto out;
    }
    if (snapshots != NULL) {
        rv = uvSnapshotLoad(uv, &snapshots[n_snapshots - 1], r->snapshot);
        if (rv != 0) {
            r->status = rv;
        }
        raft_free(snapshots);
    }
    if (segments != NULL) {
        raft_free(segments);
    }
out:
    return;
}

static void uvSnapshotGetAfterWorkCb(uv_work_t *work, int status)
{
    struct get *get = work->data;
    struct raft_io_snapshot_get *req = get->req;
    struct raft_snapshot *snapshot = get->snapshot;
    int req_status = get->status;
    struct uv *uv = get->uv;
    assert(status == 0);
    QUEUE_REMOVE(&get->queue);
    HeapFree(get);
    req->cb(req, snapshot, req_status);
    uvMaybeFireCloseCb(uv);
}

int UvSnapshotGet(struct raft_io *io,
                  struct raft_io_snapshot_get *req,
                  raft_io_snapshot_get_cb cb)
{
    struct uv *uv;
    struct get *get;
    int rv;

    uv = io->impl;
    assert(!uv->closing);

    get = HeapMalloc(sizeof *get);
    if (get == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    get->uv = uv;
    get->req = req;
    req->cb = cb;

    get->snapshot = raft_malloc(sizeof *get->snapshot);
    if (get->snapshot == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_req_alloc;
    }
    get->work.data = get;

    QUEUE_PUSH(&uv->snapshot_get_reqs, &get->queue);
    rv = uv_queue_work(uv->loop, &get->work, uvSnapshotGetWorkCb,
                       uvSnapshotGetAfterWorkCb);
    if (rv != 0) {
        QUEUE_REMOVE(&get->queue);
        Tracef(uv->tracer, "get last snapshot: %s", uv_strerror(rv));
        rv = RAFT_IOERR;
        goto err_after_snapshot_alloc;
    }

    return 0;

err_after_snapshot_alloc:
    raft_free(get->snapshot);
err_after_req_alloc:
    raft_free(get);
err:
    assert(rv != 0);
    return rv;
}
