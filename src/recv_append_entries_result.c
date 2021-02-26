#include "recv_append_entries_result.h"
#include "assert.h"
#include "configuration.h"
#include "tracing.h"
#include "recv.h"
#include "replication.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

int recvAppendEntriesResult(struct raft *r,
                            const raft_id id,
                            const char *address,
                            const struct raft_append_entries_result *result)
{
    int match;
    const struct raft_server *server;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(address != NULL);
    assert(result != NULL);

    if (r->state != RAFT_LEADER) {
        fprintf(stderr, "recvAppendEntriesResult not leader\n"); fflush(stderr);
        tracef("local server is not leader -> ignore");
        return 0;
    }

    rv = recvEnsureMatchingTerms(r, result->term, &match);
    if (rv != 0) {
        fprintf(stderr, "recvAppendEntriesResult term mismatch\n"); fflush(stderr);
        return rv;
    }

    if (match < 0) {
        fprintf(stderr, "recvAppendEntriesResult too low term\n"); fflush(stderr);
        tracef("local term is higher -> ignore ");
        return 0;
    }

    /* If we have stepped down, abort here.
     *
     * From Figure 3.1:
     *
     *   [Rules for Servers] All Servers: If RPC request or response contains
     *   term T > currentTerm: set currentTerm = T, convert to follower.
     */
    if (match > 0) {
        fprintf(stderr, "recvAppendEntriesResult higher term\n"); fflush(stderr);
        assert(r->state == RAFT_FOLLOWER);
        return 0;
    }

    assert(result->term == r->current_term);

    /* Ignore responses from servers that have been removed */
    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        fprintf(stderr, "recvAppendEntriesResult Ignoring response from server id:%llu\n", id); fflush(stderr);
        tracef("unknown server -> ignore");
        return 0;
    }
    bool is_being_promoted = r->leader_state.promotee_id != 0 &&
                        r->leader_state.promotee_id == server->id;
    if (is_being_promoted) {
        fprintf(stderr, "recvAppendEntriesResult from promotee_id %llu\n", server->id); fflush(stderr);
    }

    /* Update the progress of this server, possibly sending further entries. */
    rv = replicationUpdate(r, server, result);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef tracef
