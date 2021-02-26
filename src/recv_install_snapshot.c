#include "recv_install_snapshot.h"

#include "assert.h"
#include "convert.h"
#include "log.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

static void installSnapshotSendCb(struct raft_io_send *req, int status)
{
    (void)status;
    raft_free(req);
}

int recvInstallSnapshot(struct raft *r,
                        const raft_id id,
                        const char *address,
                        struct raft_install_snapshot *args)
{
    struct raft_io_send *req;
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    int rv;
    int match;
    bool async;
    fprintf(stderr, "recvInstallSnapshot id %llu\n", id); fflush(stderr);

    assert(address != NULL);

    result->rejected = args->last_index;
    result->last_log_index = logLastIndex(&r->log);

    rv = recvEnsureMatchingTerms(r, args->term, &match);
    if (rv != 0) {
        fprintf(stderr, "recvInstallSnapshot fail %d\n", rv); fflush(stderr);
        return rv;
    }

    if (match < 0) {
        tracef("local term is higher -> reject ");
        fprintf(stderr, "recvInstallSnapshot localt term higher id %llu\n", id); fflush(stderr);
        goto reply;
    }

    /* TODO: this logic duplicates the one in the AppendEntries handler */
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    assert(r->current_term == args->term);
    if (r->state == RAFT_CANDIDATE) {
        assert(match == 0);
        tracef("discovered leader -> step down ");
        fprintf(stderr, "recvInstallSnapshot discovered leader %llu\n", id); fflush(stderr);
        convertToFollower(r);
    }

    rv = recvUpdateLeader(r, id, address);
    if (rv != 0) {
        fprintf(stderr, "recvInstallSnapshot recvUpdateLeader failed %d\n", rv); fflush(stderr);
        return rv;
    }
    r->election_timer_start = r->io->time(r->io);

    rv = replicationInstallSnapshot(r, args, &result->rejected, &async);
    if (rv != 0) {
        fprintf(stderr, "recvInstallSnapshot replicationInstallSnapshot failed %d\n", rv); fflush(stderr);
        return rv;
    }

    if (async) {
        fprintf(stderr, "recvInstallSnapshot async %d\n", rv); fflush(stderr);
        return 0;
    }

    if (result->rejected == 0) {
        /* Echo back to the leader the point that we reached. */
        result->last_log_index = args->last_index;
    }

   fprintf(stderr, "recvInstallSnapshot reply %d\n", rv); fflush(stderr);
reply:
    result->term = r->current_term;

    /* Free the snapshot data. */
    raft_configuration_close(&args->conf);
    raft_free(args->data.base);

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = id;
    message.server_address = address;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->data = r;

    rv = r->io->send(r->io, req, &message, installSnapshotSendCb);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}

#undef tracef
