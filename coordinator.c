/*
 * coordinator.c -- YOU IMPLEMENT THE TODOs IN THIS FILE.
 */
#include "common.h"

#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define POLL_MS 50

enum phase { PH_CP, PH_RECOVER_REDUCER, PH_RECOVER_MAPPER, PH_LAST_CP, PH_EXITING };

struct wstate {
    long last_hb;       /* time of the last heartbeat from the current process */
    int  heard;         /* current process has sent a heartbeat (its sockets are up) */
    long last_cp;       /* newest checkpoint this worker acknowledged */
    /* TODO: add whatever else you need per worker */
    int done;
    int waiting_to_exit;
    int recovered;
    int recovery_sent;
};

static const config_t *cfg;
static job_t         *job;
static struct wstate  ws[MAX_WORKERS];
static enum phase     phase;
static int            rid;              /* current recovery id */
static long           next_cp_id = 1;   /* never reused */
static long           last_cp_trigger_time;
static long           checkpoint_in_progress = NO_CHECKPOINT;
static long           recovery_checkpoint = NO_CHECKPOINT;

static const char *phase_name[] = {
    "CP", "RECOVER_REDUCER", "RECOVER_MAPPER", "LAST_CP", "EXITING",
};

static int is_mapper(int w) { return w < cfg->M; }

static const char *wname(int w)
{
    static char buf[4][32];
    static int k;
    char *b = buf[k++ % 4];
    snprintf(b, sizeof buf[0], "%s %d", is_mapper(w) ? "mapper" : "reducer",
             is_mapper(w) ? w : w - cfg->M);
    return b;
}

/* PROVIDED: change phase (and log it). Use this instead of assigning phase. */
static void set_phase(enum phase p)
{
    wlog("phase %s -> %s (rid %d)", phase_name[phase], phase_name[p], rid);
    phase = p;
}

/* Send a message of the given type to worker w, tagged with our recovery id. */
static void send_to(int w, int type, long cp)
{
    msg_t m;
    memset(&m, 0, sizeof m);
    m.type = type;
    m.role = ROLE_COORDINATOR;
    m.recovery_id = rid;
    m.checkpoint_id = cp;
    int role = is_mapper(w) ? ROLE_MAPPER : ROLE_REDUCER;
    int id = is_mapper(w) ? w : w - cfg->M;
    wlog("-> %s: %s cp=%ld rid=%d", wname(w), msg_type_name(type), cp, rid);
    udp_send(job->fd, worker_port(cfg->base_port, cfg->M, role, id), &m);
}

static int all_mappers_done(void)
{
    for (int i = 0; i < cfg->M; i++)
        if (!ws[i].done)
            return 0;
    return 1;
}

static int checkpoint_complete(void)
{
    for (int i = 0; i < job->n; i++)
        if (ws[i].last_cp != checkpoint_in_progress)
            return 0;
    return 1;
}

static void start_last_checkpoint(void)
{
    set_phase(PH_LAST_CP);
    for (int i = 0; i < cfg->M; i++)
        send_to(i, MT_CHECKPOINT, LAST_CHECKPOINT_ID);
}

static int all_mappers_recovered(void)
{
    for (int i = 0; i < cfg->M; i++)
        if (!ws[i].recovered)
            return 0;
    return 1;
}

static int all_reducers_recovered(void)
{
    for (int i = cfg->M; i < cfg->M + cfg->R; i++)
        if (!ws[i].recovered)
            return 0;
    return 1;
}

/*
 * TODO (Task 3)
 */
static void start_recovery(void)
{
    set_phase(PH_RECOVER_REDUCER);

    long restore_cp = LONG_MAX;
    for (int i = 0; i < job->n; i++) {
        if (ws[i].last_cp != NO_CHECKPOINT && ws[i].last_cp < restore_cp)
            restore_cp = ws[i].last_cp;
        ws[i].done = 0;
        ws[i].waiting_to_exit = 0;
        ws[i].recovered = 0;
        ws[i].recovery_sent = 0;
    }
    recovery_checkpoint = restore_cp == LONG_MAX ? NO_CHECKPOINT : restore_cp;
    checkpoint_in_progress = NO_CHECKPOINT;
    for (int i = 0; i < job->n; i++)
        ws[i].last_cp = recovery_checkpoint;

    for (int i = job->M; i < job->n; i++) {
        send_to(i, MT_RECOVER, recovery_checkpoint);
        ws[i].recovery_sent = ws[i].heard;
    }
}

/* One message from a worker. */
static void handle(const msg_t *m, long now)
{
    if ((m->role != ROLE_MAPPER && m->role != ROLE_REDUCER) || m->id < 0 ||
        m->id >= (m->role == ROLE_MAPPER ? cfg->M : cfg->R))
        return;
    int w = worker_index(cfg->M, m->role, m->id);

    if (m->type == MT_HEARTBEAT) {
        /* Ignore heartbeats from a process we already replaced. */
        if (m->count == (int64_t)job->pid[w]) {
            if (!ws[w].heard)
                wlog("%s is up (pid %ld)", wname(w), (long)m->count);
            ws[w].last_hb = now;
            ws[w].heard = 1;
            if (!ws[w].recovery_sent && !ws[w].recovered &&
                ((phase == PH_RECOVER_REDUCER && w >= cfg->M) ||
                 (phase == PH_RECOVER_MAPPER && w < cfg->M))) {
                send_to(w, MT_RECOVER, recovery_checkpoint);
                ws[w].recovery_sent = 1;
            }
        }
        return;
    }

    wlog("<- %s: %s cp=%ld rid=%d (mine: rid=%d, phase %s)", wname(w),
         msg_type_name(m->type), (long)m->checkpoint_id, m->recovery_id,
         rid, phase_name[phase]);

    if (m->recovery_id != rid)
        return;

    switch (m->type) {
    case MT_CHECKPOINT_ACK:
        /* TODO (Task 2) */
        if (phase != PH_CP || m->checkpoint_id <= 0)
            break;
        ws[w].last_cp = m->checkpoint_id;
        if (m->checkpoint_id == checkpoint_in_progress &&
            checkpoint_complete()) {
            checkpoint_in_progress = NO_CHECKPOINT;
            if (all_mappers_done())
                start_last_checkpoint();
        }
        break;
    case MT_LAST_CHECKPOINT_ACK:
        /* TODO (Task 1) */
        if (phase != PH_LAST_CP || m->checkpoint_id != LAST_CHECKPOINT_ID)
            break;
        ws[w].waiting_to_exit = 1;

        for (int i = 0; i < job->n; i++)
            if (!ws[i].waiting_to_exit)
                return;

        set_phase(PH_EXITING);

        for (int i = 0; i < job->n; i++) {
            send_to(i, MT_EXIT, 0);
        }
        break;
    case MT_DONE:
        /* TODO (Task 1) */
        if (phase != PH_CP || m->role != ROLE_MAPPER)
            break;
        ws[w].done = 1;
        if (all_mappers_done() && checkpoint_in_progress == NO_CHECKPOINT)
            start_last_checkpoint();
        break;
    case MT_RECOVERY_ACK:
        /* TODO (Task 3) */
        if ((phase != PH_RECOVER_REDUCER && phase != PH_RECOVER_MAPPER) ||
            m->checkpoint_id != recovery_checkpoint)
            break;
        ws[w].recovered = 1;
        if (m->role == ROLE_REDUCER && all_reducers_recovered()) {
            set_phase(PH_RECOVER_MAPPER);
            for (int i = 0; i < job->M; i++) {
                send_to(i, MT_RECOVER, recovery_checkpoint);
                ws[i].recovery_sent = 1;
            }
        } else if (m-> role == ROLE_MAPPER && all_mappers_recovered()) {
            set_phase(PH_CP);
        }
        break;
    default:
        break;
    }
}

/* Returns 0 when the job is complete; any other value is a fatal error. */
int coordinator_loop(const config_t *c, job_t *j)
{
    cfg = c;
    job = j;
    long now = now_ms();
    for (int w = 0; w < job->n; w++) {
        memset(&ws[w], 0, sizeof ws[w]);
        ws[w].last_hb = now;
        ws[w].last_cp = NO_CHECKPOINT;
        ws[w].done = 0;
        ws[w].waiting_to_exit = 0;
    }
    phase = PH_CP;
    rid = 0;
    last_cp_trigger_time = now;
    checkpoint_in_progress = NO_CHECKPOINT;
    recovery_checkpoint = NO_CHECKPOINT;

    for (;;) {
        chaos_tick(job);        /* PROVIDED: do not remove */

        struct pollfd p = { .fd = job->fd, .events = POLLIN, .revents = 0 };
        if (poll(&p, 1, POLL_MS) < 0 && errno != EINTR) {
            perror("poll");
            return 1;
        }
        now = now_ms();

        if (phase == PH_CP && checkpoint_in_progress == NO_CHECKPOINT &&
            cfg->interval_ms > 0 &&
            now - last_cp_trigger_time >= cfg->interval_ms) {
            last_cp_trigger_time = now;
            checkpoint_in_progress = next_cp_id;
            for (int i = 0; i < cfg->M; i++)
                send_to(i, MT_CHECKPOINT, checkpoint_in_progress);
            next_cp_id++;
        }

        msg_t m;
        int r;
        while ((r = udp_recv(job->fd, &m)) == 1)
            handle(&m, now);
        if (r < 0) {
            perror("udp_recv");
            return 1;
        }

        reap_children(job);     /* bookkeeping only */

        if (phase == PH_EXITING) {
            /* TODO (Task 1). */
            int all_exited = 1;
            for (int i = 0; i < job->n; i++) {
                if (!job->exited[i]) {
                    all_exited = 0;
                    break;
                }
            }
            if (all_exited) return 0;
            continue;
        }

        /* TODO (Task 3) */
        if (phase != PH_EXITING) {
            for (int i = 0; i < job->n; i++) {
                if (now - ws[i].last_hb > HEARTBEAT_TIMEOUT_MS) {
                    rid++;
                    if (respawn_worker(job, cfg, i) != 0)
                        return 1;
                    ws[i].heard = 0;
                    ws[i].last_hb = now;
                    start_recovery();
                    break;
                }
            }
        }
    }
}
