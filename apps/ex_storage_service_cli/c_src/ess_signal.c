/* Invocation-owned POSIX SIGINT bridge. No BEAM API runs in the signal handler. */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700
#include <erl_nif.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <termios.h>
#include <string.h>
#include <unistd.h>
#include <spawn.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/wait.h>

_Static_assert(ATOMIC_INT_LOCK_FREE == 2, "SIGINT bridge requires lock-free integer atomics");

typedef struct {
    int read_fd, write_fd, closed, selected, monitored;
    ErlNifPid owner;
    ErlNifMonitor monitor;
} signal_resource;

static ErlNifResourceType *resource_type;
static ErlNifResourceType *directory_type;
static ErlNifResourceType *terminal_type;
static ErlNifResourceType *pager_type;
static ErlNifMutex *mutex;
static signal_resource *active_resource;
static struct sigaction previous_action;
static struct sigaction previous_term_action;
static int has_previous_action;
static _Atomic int active_write_fd = -1;
static _Atomic unsigned int inflight_handlers = 0;
static _Atomic unsigned int pending_signals = 0;
static _Atomic int terminal_active = 0;

static ERL_NIF_TERM atom(ErlNifEnv *env, const char *name) { return enif_make_atom(env, name); }
static ERL_NIF_TERM error(ErlNifEnv *env, const char *code) {
    return enif_make_tuple2(env, atom(env, "error"), atom(env, code));
}

static void sigint_handler(int number) {
    int saved_errno = errno;
    atomic_fetch_add_explicit(&inflight_handlers, 1, memory_order_seq_cst);
    int fd = atomic_load_explicit(&active_write_fd, memory_order_seq_cst);
    if (fd >= 0) {
        atomic_fetch_or_explicit(&pending_signals, number == SIGTERM ? 2 : 1, memory_order_seq_cst);
        unsigned char value = 1;
        /* Full pipe coalesces SIGINT: at least one pending byte already exists. */
        (void)write(fd, &value, sizeof(value));
    }
    atomic_fetch_sub_explicit(&inflight_handlers, 1, memory_order_seq_cst);
    errno = saved_errno;
}

static void close_fds(signal_resource *resource) {
    enif_mutex_lock(mutex);
    if (resource->read_fd >= 0) close(resource->read_fd);
    if (resource->write_fd >= 0) close(resource->write_fd);
    resource->read_fd = resource->write_fd = -1;
    enif_mutex_unlock(mutex);
}

static void stop(ErlNifEnv *env, void *object, ErlNifEvent event, int direct) {
    (void)env; (void)event; (void)direct;
    close_fds(object);
}

static void shutdown_resource(ErlNifEnv *env, signal_resource *resource) {
    enif_mutex_lock(mutex);
    if (resource->closed) { enif_mutex_unlock(mutex); return; }
    resource->closed = 1;
    if (active_resource == resource) {
        atomic_store_explicit(&active_write_fd, -1, memory_order_seq_cst);
        (void)sigaction(SIGINT, &previous_action, NULL);
        (void)sigaction(SIGTERM, &previous_term_action, NULL);
        /* A handler that already read the old fd must finish before fd reuse. */
        while (atomic_load_explicit(&inflight_handlers, memory_order_seq_cst) != 0) sched_yield();
        active_resource = NULL;
    }
    int selected = resource->selected;
    int fd = resource->read_fd;
    resource->selected = 0;
    if (resource->monitored) {
        resource->monitored = 0;
        (void)enif_demonitor_process(env, resource, &resource->monitor);
    }
    enif_mutex_unlock(mutex);
    if (selected) {
        int result = enif_select(env, fd, ERL_NIF_SELECT_STOP, resource, NULL, atom(env, "undefined"));
        if (result & (ERL_NIF_SELECT_FAILED | ERL_NIF_SELECT_INVALID_EVENT | ERL_NIF_SELECT_NOTSUP)) close_fds(resource);
    } else close_fds(resource);
}

static void destructor(ErlNifEnv *env, void *object) { shutdown_resource(env, object); }
static void down(ErlNifEnv *env, void *object, ErlNifPid *pid, ErlNifMonitor *monitor) {
    (void)pid; (void)monitor;
    signal_resource *resource = object;
    enif_mutex_lock(mutex);
    resource->monitored = 0;
    enif_mutex_unlock(mutex);
    shutdown_resource(env, resource);
}

static int select_read(ErlNifEnv *env, signal_resource *resource) {
    int result = enif_select(env, resource->read_fd, ERL_NIF_SELECT_READ, resource, &resource->owner, atom(env, "ess_sigint"));
    return !(result & (ERL_NIF_SELECT_FAILED | ERL_NIF_SELECT_INVALID_EVENT | ERL_NIF_SELECT_NOTSUP));
}

static ERL_NIF_TERM start(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifPid owner, caller;
    if (!enif_get_local_pid(env, argv[0], &owner) || !enif_self(env, &caller) || enif_compare_pids(&owner, &caller) != 0) return error(env, "signal_owner_required");
    enif_mutex_lock(mutex);
    if (active_resource != NULL) { enif_mutex_unlock(mutex); return error(env, "signal_owner_exists"); }
    signal_resource *resource = enif_alloc_resource(resource_type, sizeof(*resource));
    if (!resource) { enif_mutex_unlock(mutex); return error(env, "signal_setup_failed"); }
    memset(resource, 0, sizeof(*resource));
    resource->read_fd = resource->write_fd = -1;
    resource->owner = owner;
    int fds[2];
    if (pipe(fds) != 0) { enif_mutex_unlock(mutex); enif_release_resource(resource); return error(env, "signal_setup_failed"); }
    resource->read_fd = fds[0]; resource->write_fd = fds[1];
    if (fcntl(fds[0], F_SETFL, O_NONBLOCK) != 0 || fcntl(fds[1], F_SETFL, O_NONBLOCK) != 0 ||
        fcntl(fds[0], F_SETFD, FD_CLOEXEC) != 0 || fcntl(fds[1], F_SETFD, FD_CLOEXEC) != 0 ||
        enif_monitor_process(env, resource, &owner, &resource->monitor) != 0) {
        enif_mutex_unlock(mutex); enif_release_resource(resource); return error(env, "signal_setup_failed");
    }
    resource->monitored = 1;
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = sigint_handler;
    action.sa_flags = SA_RESTART;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, &previous_action) != 0) {
        enif_mutex_unlock(mutex); enif_release_resource(resource); return error(env, "signal_setup_failed");
    }
    if (sigaction(SIGTERM, &action, &previous_term_action) != 0) {
        (void)sigaction(SIGINT, &previous_action, NULL);
        enif_mutex_unlock(mutex); enif_release_resource(resource); return error(env, "signal_setup_failed");
    }
    has_previous_action = 1;
    active_resource = resource;
    atomic_store_explicit(&pending_signals, 0, memory_order_seq_cst);
    atomic_store_explicit(&active_write_fd, resource->write_fd, memory_order_seq_cst);
    resource->selected = 1;
    int selected = select_read(env, resource);
    enif_mutex_unlock(mutex);
    if (!selected) { shutdown_resource(env, resource); enif_release_resource(resource); return error(env, "signal_select_failed"); }
    ERL_NIF_TERM term = enif_make_resource(env, resource);
    enif_release_resource(resource);
    return enif_make_tuple2(env, atom(env, "ok"), term);
}

static ERL_NIF_TERM drain(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    signal_resource *resource;
    ErlNifPid caller;
    if (!enif_get_resource(env, argv[0], resource_type, (void **)&resource)) return enif_make_badarg(env);
    if (!enif_self(env, &caller) || enif_compare_pids(&caller, &resource->owner) != 0) return error(env, "signal_owner_required");
    enif_mutex_lock(mutex);
    if (resource->closed) { enif_mutex_unlock(mutex); return error(env, "signal_resource_closed"); }
    unsigned char bytes[256];
    ssize_t count;
    while ((count = read(resource->read_fd, bytes, sizeof(bytes))) > 0) {}
    if (count < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        enif_mutex_unlock(mutex); return error(env, "signal_read_failed");
    }
    unsigned int pending = atomic_exchange_explicit(&pending_signals, 0, memory_order_seq_cst);
    int selected = select_read(env, resource);
    enif_mutex_unlock(mutex);
    if (!selected) return error(env, "signal_select_failed");
    ERL_NIF_TERM signals = enif_make_list(env, 0);
    if (pending & 2) signals = enif_make_list_cell(env, atom(env, "sigterm"), signals);
    if (pending & 1) signals = enif_make_list_cell(env, atom(env, "sigint"), signals);
    return signals;
}

static ERL_NIF_TERM close_resource(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    signal_resource *resource;
    if (!enif_get_resource(env, argv[0], resource_type, (void **)&resource)) return enif_make_badarg(env);
    shutdown_resource(env, resource);
    return atom(env, "ok");
}

static int disposition_restored(int signal, const struct sigaction *previous) {
    struct sigaction current;
    /* The kernel adds private flags (Linux SA_RESTORER) when restoring SIG_DFL. */
    int user_flags = SA_NOCLDSTOP | SA_NOCLDWAIT | SA_NODEFER | SA_ONSTACK |
        SA_RESETHAND | SA_RESTART | SA_SIGINFO;
    int valid = sigaction(signal, NULL, &current) == 0 &&
        current.sa_handler == previous->sa_handler &&
        (current.sa_flags & user_flags) == (previous->sa_flags & user_flags);
#ifdef SIGRTMAX
    int maximum_signal = SIGRTMAX;
#else
    int maximum_signal = 31;
#endif
    if (valid) for (int number = 1; number <= maximum_signal; number++) {
        if (sigismember(&current.sa_mask, number) != sigismember(&previous->sa_mask, number)) { valid = 0; break; }
    }
    return valid;
}

static ERL_NIF_TERM restored(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    enif_mutex_lock(mutex);
    int valid = has_previous_action && active_resource == NULL &&
        disposition_restored(SIGINT, &previous_action) && disposition_restored(SIGTERM, &previous_term_action);
    enif_mutex_unlock(mutex);
    return atom(env, valid ? "true" : "false");
}

typedef struct {
    DIR *directory;
    ErlNifMutex *lock;
    ErlNifPid owner;
    ErlNifMonitor monitor;
    int monitored;
} directory_resource;

static void directory_close(directory_resource *resource) {
    enif_mutex_lock(resource->lock);
    if (resource->directory) { closedir(resource->directory); resource->directory = NULL; }
    enif_mutex_unlock(resource->lock);
}

static void directory_destructor(ErlNifEnv *env, void *object) {
    (void)env;
    directory_resource *resource = object;
    if (resource->lock) { directory_close(resource); enif_mutex_destroy(resource->lock); }
}

static void directory_down(ErlNifEnv *env, void *object, ErlNifPid *pid, ErlNifMonitor *monitor) {
    (void)env; (void)pid; (void)monitor;
    directory_close(object);
}

static ERL_NIF_TERM directory_error(ErlNifEnv *env, int code) {
    const char *name;
    switch (code) {
        case ENOENT: name = "enoent"; break;
        case EACCES: name = "eacces"; break;
        case ENOTDIR: name = "enotdir"; break;
        case ELOOP: name = "eloop"; break;
        case EMFILE: case ENFILE: name = "emfile"; break;
        case ENOMEM: name = "enomem"; break;
        default: name = "eio"; break;
    }
    return error(env, name);
}

static ERL_NIF_TERM open_directory(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary path;
    int follow = enif_is_identical(argv[1], atom(env, "true"));
    if (!enif_inspect_binary(env, argv[0], &path) || memchr(path.data, 0, path.size) ||
        (!follow && !enif_is_identical(argv[1], atom(env, "false")))) return enif_make_badarg(env);
    char *name = enif_alloc(path.size + 1);
    if (!name) return directory_error(env, ENOMEM);
    memcpy(name, path.data, path.size); name[path.size] = 0;
    int flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
    if (!follow) flags |= O_NOFOLLOW;
    int fd = open(name, flags);
    int saved_errno = errno;
    enif_free(name);
    if (fd < 0) return directory_error(env, saved_errno);
    DIR *directory = fdopendir(fd);
    if (!directory) { saved_errno = errno; close(fd); return directory_error(env, saved_errno); }
    directory_resource *resource = enif_alloc_resource(directory_type, sizeof(*resource));
    if (!resource) { closedir(directory); return directory_error(env, ENOMEM); }
    memset(resource, 0, sizeof(*resource));
    resource->lock = enif_mutex_create("ess_directory");
    if (!resource->lock) { closedir(directory); enif_release_resource(resource); return directory_error(env, ENOMEM); }
    resource->directory = directory;
    enif_self(env, &resource->owner);
    if (enif_monitor_process(env, resource, &resource->owner, &resource->monitor) != 0) {
        enif_release_resource(resource); return error(env, "directory_owner_unavailable");
    }
    resource->monitored = 1;
    ERL_NIF_TERM term = enif_make_resource(env, resource);
    enif_release_resource(resource);
    return enif_make_tuple2(env, atom(env, "ok"), term);
}

static ERL_NIF_TERM read_directory(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    directory_resource *resource;
    unsigned int maximum;
    ErlNifPid caller;
    if (!enif_get_resource(env, argv[0], directory_type, (void **)&resource) ||
        !enif_get_uint(env, argv[1], &maximum) || maximum == 0 || maximum > 1024) return enif_make_badarg(env);
    enif_self(env, &caller);
    if (enif_compare_pids(&caller, &resource->owner) != 0) return error(env, "directory_owner_required");
    enif_mutex_lock(resource->lock);
    if (!resource->directory) { enif_mutex_unlock(resource->lock); return error(env, "directory_closed"); }
    ERL_NIF_TERM entries = enif_make_list(env, 0);
    unsigned int count = 0;
    while (count < maximum) {
        errno = 0;
        struct dirent *entry = readdir(resource->directory);
        if (!entry) {
            int code = errno;
            enif_mutex_unlock(resource->lock);
            if (code) return directory_error(env, code);
            if (count == 0) return atom(env, "eof");
            enif_make_reverse_list(env, entries, &entries);
            return enif_make_tuple2(env, atom(env, "ok"), entries);
        }
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
        ERL_NIF_TERM name;
        size_t length = strlen(entry->d_name);
        unsigned char *bytes = enif_make_new_binary(env, length, &name);
        if (!bytes) { enif_mutex_unlock(resource->lock); return directory_error(env, ENOMEM); }
        memcpy(bytes, entry->d_name, length);
        entries = enif_make_list_cell(env, name, entries);
        count++;
    }
    enif_mutex_unlock(resource->lock);
    enif_make_reverse_list(env, entries, &entries);
    return enif_make_tuple2(env, atom(env, "ok"), entries);
}

static ERL_NIF_TERM close_directory(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    directory_resource *resource;
    if (!enif_get_resource(env, argv[0], directory_type, (void **)&resource)) return enif_make_badarg(env);
    directory_close(resource);
    return atom(env, "ok");
}

typedef struct {
    int fd;
    int claimed;
    struct termios original;
    ErlNifMutex *lock;
    ErlNifMonitor monitor;
} terminal_resource;

static void terminal_close(terminal_resource *resource) {
    enif_mutex_lock(resource->lock);
    if (resource->fd >= 0) {
        (void)tcsetattr(resource->fd, TCSANOW, &resource->original);
        close(resource->fd); resource->fd = -1;
    }
    if (resource->claimed) {
        resource->claimed = 0;
        atomic_store_explicit(&terminal_active, 0, memory_order_seq_cst);
    }
    enif_mutex_unlock(resource->lock);
}

static void terminal_destructor(ErlNifEnv *env, void *object) {
    (void)env;
    terminal_resource *resource = object;
    if (resource->lock) { terminal_close(resource); enif_mutex_destroy(resource->lock); }
}

static void terminal_down(ErlNifEnv *env, void *object, ErlNifPid *pid, ErlNifMonitor *monitor) {
    (void)env; (void)pid; (void)monitor;
    terminal_close(object);
}

static ERL_NIF_TERM suppress_stdin_echo(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc; (void)argv;
    if (!isatty(STDIN_FILENO)) return error(env, "terminal_unavailable");
    terminal_resource *resource = enif_alloc_resource(terminal_type, sizeof(*resource));
    if (!resource) return error(env, "terminal_unavailable");
    memset(resource, 0, sizeof(*resource));
    resource->fd = -1;
    resource->lock = enif_mutex_create("ess_terminal");
    if (!resource->lock) { enif_release_resource(resource); return error(env, "terminal_unavailable"); }
    int expected = 0;
    if (!atomic_compare_exchange_strong_explicit(&terminal_active, &expected, 1, memory_order_seq_cst, memory_order_seq_cst)) {
        enif_release_resource(resource); return error(env, "terminal_busy");
    }
    resource->claimed = 1;
    int fd = fcntl(STDIN_FILENO, F_DUPFD_CLOEXEC, 3);
    if (fd < 0) { enif_release_resource(resource); return error(env, "terminal_unavailable"); }
    if (tcgetattr(fd, &resource->original) != 0) { close(fd); enif_release_resource(resource); return error(env, "terminal_unavailable"); }
    struct termios hidden = resource->original;
    hidden.c_lflag &= ~(ECHO | ECHONL);
    resource->fd = fd;
    ErlNifPid owner;
    enif_self(env, &owner);
    if (tcsetattr(fd, TCSANOW, &hidden) != 0 ||
        enif_monitor_process(env, resource, &owner, &resource->monitor) != 0) {
        enif_release_resource(resource); return error(env, "terminal_unavailable");
    }
    ERL_NIF_TERM term = enif_make_resource(env, resource);
    enif_release_resource(resource);
    return enif_make_tuple2(env, atom(env, "ok"), term);
}

static ERL_NIF_TERM restore_terminal(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    terminal_resource *resource;
    if (!enif_get_resource(env, argv[0], terminal_type, (void **)&resource)) return enif_make_badarg(env);
    terminal_close(resource);
    return atom(env, "ok");
}


/* A dedicated reaper owns only its posix_spawn child. No global SIGCHLD changes.
 * Keeping the resource alive lets DOWN close the lifetime pipe even while the
 * caller exits; the single-threaded helper restores the tty before reporting. */
typedef struct {
    int control, status, finished, exit_status;
    pid_t pid;
    ErlNifPid owner;
    ErlNifMonitor monitor;
    ErlNifMutex *lock;
    pthread_t thread;
} pager_resource;

static void pager_cancel(pager_resource *resource) {
    enif_mutex_lock(resource->lock);
    if (resource->control >= 0) { close(resource->control); resource->control = -1; }
    enif_mutex_unlock(resource->lock);
}

static void pager_destructor(ErlNifEnv *env, void *object) {
    (void)env;
    pager_resource *resource = object;
    if (resource->lock) {
        pager_cancel(resource);
        if (resource->status >= 0) close(resource->status);
        enif_mutex_destroy(resource->lock);
    }
}

static void pager_down(ErlNifEnv *env, void *object, ErlNifPid *pid, ErlNifMonitor *monitor) {
    (void)env; (void)pid; (void)monitor;
    pager_cancel(object);
}

#ifdef __linux__
static void *pager_reap(void *object) {
    pager_resource *resource = object;
    int reported = 125;
    size_t received = 0;
    while (received < sizeof(reported)) {
        ssize_t count = read(resource->status, (char *)&reported + received, sizeof(reported) - received);
        if (count > 0) received += (size_t)count;
        else if (count < 0 && errno == EINTR) continue;
        else break;
    }
    int status;
    while (waitpid(resource->pid, &status, 0) < 0 && errno == EINTR) {}
    enif_mutex_lock(resource->lock);
    close(resource->status); resource->status = -1;
    if (resource->control >= 0) { close(resource->control); resource->control = -1; }
    resource->exit_status = received == sizeof(reported) ? reported : 125;
    resource->finished = 1;
    enif_mutex_unlock(resource->lock);
    enif_release_resource(resource);
    return NULL;
}

static void free_strings(char **strings) {
    if (!strings) return;
    for (unsigned int index = 0; strings[index]; index++) enif_free(strings[index]);
    enif_free(strings);
}

static char **pager_strings(ErlNifEnv *env, ERL_NIF_TERM list, unsigned int maximum) {
    unsigned int length;
    if (!enif_get_list_length(env, list, &length) || length > maximum) return NULL;
    char **strings = enif_alloc((length + 1) * sizeof(char *));
    if (!strings) return NULL;
    memset(strings, 0, (length + 1) * sizeof(char *));
    size_t total = 0;
    for (unsigned int index = 0; index < length; index++) {
        ERL_NIF_TERM head, tail;
        ErlNifBinary bytes;
        if (!enif_get_list_cell(env, list, &head, &tail) || !enif_inspect_binary(env, head, &bytes) ||
            memchr(bytes.data, 0, bytes.size) || (total += bytes.size) > 1048576) {
            free_strings(strings); return NULL;
        }
        strings[index] = enif_alloc(bytes.size + 1);
        if (!strings[index]) { free_strings(strings); return NULL; }
        memcpy(strings[index], bytes.data, bytes.size); strings[index][bytes.size] = 0;
        list = tail;
    }
    return strings;
}

#endif

static ERL_NIF_TERM pager_start(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
#ifndef __linux__
    (void)argv;
    return error(env, "pager_native_unavailable");
#else
    ErlNifPid owner;
    enif_self(env, &owner);
    char **arguments = pager_strings(env, argv[0], 128);
    char **environment = pager_strings(env, argv[1], 4096);
    if (!arguments || !arguments[0] || !arguments[1] || !arguments[2] || !environment) {
        free_strings(arguments); free_strings(environment); return error(env, "pager_setup_failed");
    }
    int control[2] = {-1, -1}, status[2] = {-1, -1};
    pager_resource *resource = enif_alloc_resource(pager_type, sizeof(*resource));
    if (!resource) { free_strings(arguments); free_strings(environment); return error(env, "pager_setup_failed"); }
    memset(resource, 0, sizeof(*resource));
    resource->control = resource->status = -1;
    resource->owner = owner;
    resource->lock = enif_mutex_create("ess_pager");
    int result = -1;
    if (!resource->lock || pipe2(control, O_CLOEXEC) != 0 || pipe2(status, O_CLOEXEC) != 0) goto finished;
    /* Reserve source descriptors above the fixed child control/status slots. */
    int read_control = fcntl(control[0], F_DUPFD_CLOEXEC, 5);
    int write_status = fcntl(status[1], F_DUPFD_CLOEXEC, 5);
    if (read_control < 0 || write_status < 0) {
        if (read_control >= 0) close(read_control);
        if (write_status >= 0) close(write_status);
        goto finished;
    }
    posix_spawn_file_actions_t actions;
    posix_spawnattr_t attributes;
    int action_ready = posix_spawn_file_actions_init(&actions) == 0;
    int attr_ready = posix_spawnattr_init(&attributes) == 0;
    if (action_ready && attr_ready) {
        sigset_t empty, defaults;
        sigemptyset(&empty); sigemptyset(&defaults);
        sigaddset(&defaults, SIGINT); sigaddset(&defaults, SIGTERM);
        sigaddset(&defaults, SIGTTOU); sigaddset(&defaults, SIGTTIN);
        sigaddset(&defaults, SIGTSTP); sigaddset(&defaults, SIGPIPE);
        sigaddset(&defaults, SIGCHLD);
        result = posix_spawn_file_actions_adddup2(&actions, read_control, 3);
        if (!result) result = posix_spawn_file_actions_adddup2(&actions, write_status, 4);
        if (!result) result = posix_spawnattr_setpgroup(&attributes, 0);
        if (!result) result = posix_spawnattr_setsigmask(&attributes, &empty);
        if (!result) result = posix_spawnattr_setsigdefault(&attributes, &defaults);
        if (!result) result = posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF);
        if (!result) result = posix_spawn(&resource->pid, arguments[0], &actions, &attributes, arguments, environment);
    }
    if (action_ready) posix_spawn_file_actions_destroy(&actions);
    if (attr_ready) posix_spawnattr_destroy(&attributes);
    close(read_control); close(write_status);
    if (result != 0) goto finished;
    resource->control = control[1]; control[1] = -1;
    resource->status = status[0]; status[0] = -1;
    close(control[0]); control[0] = -1;
    close(status[1]); status[1] = -1;
    if (enif_monitor_process(env, resource, &owner, &resource->monitor) != 0) { result = -1; goto spawned_failure; }
    enif_keep_resource(resource);
    pthread_attr_t thread_attributes;
    int thread_ready = pthread_attr_init(&thread_attributes) == 0;
    int thread_result = thread_ready ? pthread_attr_setdetachstate(&thread_attributes, PTHREAD_CREATE_DETACHED) : -1;
    if (!thread_result) thread_result = pthread_create(&resource->thread, &thread_attributes, pager_reap, resource);
    if (thread_ready) pthread_attr_destroy(&thread_attributes);
    if (thread_result != 0) {
        enif_release_resource(resource); result = -1; goto spawned_failure;
    }
    goto finished;
spawned_failure:
    pager_cancel(resource);
    while (waitpid(resource->pid, NULL, 0) < 0 && errno == EINTR) {}
finished:
    for (unsigned int index = 0; index < 2; index++) {
        if (control[index] >= 0) close(control[index]);
        if (status[index] >= 0) close(status[index]);
    }
    free_strings(arguments); free_strings(environment);
    ERL_NIF_TERM term = result == 0 ? enif_make_tuple2(env, atom(env, "ok"), enif_make_resource(env, resource)) : error(env, "pager_setup_failed");
    enif_release_resource(resource);
    return term;
#endif
}

static ERL_NIF_TERM pager_poll(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    pager_resource *resource;
    ErlNifPid caller;
    if (!enif_get_resource(env, argv[0], pager_type, (void **)&resource)) return enif_make_badarg(env);
    enif_self(env, &caller);
    if (enif_compare_pids(&caller, &resource->owner) != 0) return error(env, "pager_owner_required");
    enif_mutex_lock(resource->lock);
    int finished = resource->finished, status = resource->exit_status;
    enif_mutex_unlock(resource->lock);
    return finished ? enif_make_tuple2(env, atom(env, "exit"), enif_make_int(env, status)) : atom(env, "running");
}

static ERL_NIF_TERM pager_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    pager_resource *resource;
    ErlNifPid caller;
    if (!enif_get_resource(env, argv[0], pager_type, (void **)&resource)) return enif_make_badarg(env);
    enif_self(env, &caller);
    if (enif_compare_pids(&caller, &resource->owner) != 0) return error(env, "pager_owner_required");
    pager_cancel(resource);
    return atom(env, "ok");
}

static int load(ErlNifEnv *env, void **data, ERL_NIF_TERM info) {
    (void)data; (void)info;
    ErlNifResourceTypeInit callbacks;
    memset(&callbacks, 0, sizeof(callbacks));
    callbacks.dtor = destructor; callbacks.stop = stop; callbacks.down = down; callbacks.members = 3;
    mutex = enif_mutex_create("ess_sigint");
    if (!mutex) return -1;
    resource_type = enif_open_resource_type_x(env, "ess_sigint_resource", &callbacks, ERL_NIF_RT_CREATE, NULL);
    memset(&callbacks, 0, sizeof(callbacks));
    callbacks.dtor = directory_destructor; callbacks.down = directory_down; callbacks.members = 3;
    directory_type = enif_open_resource_type_x(env, "ess_directory_resource", &callbacks, ERL_NIF_RT_CREATE, NULL);
    memset(&callbacks, 0, sizeof(callbacks));
    callbacks.dtor = terminal_destructor; callbacks.down = terminal_down; callbacks.members = 3;
    terminal_type = enif_open_resource_type_x(env, "ess_terminal_resource", &callbacks, ERL_NIF_RT_CREATE, NULL);
    memset(&callbacks, 0, sizeof(callbacks));
    callbacks.dtor = pager_destructor; callbacks.down = pager_down; callbacks.members = 3;
    pager_type = enif_open_resource_type_x(env, "ess_pager_resource", &callbacks, ERL_NIF_RT_CREATE, NULL);
    return resource_type && directory_type && terminal_type && pager_type ? 0 : -1;
}

static ErlNifFunc functions[] = {
    {"nif_start", 1, start, 0}, {"nif_drain", 1, drain, 0},
    {"nif_close", 1, close_resource, 0}, {"nif_restored", 0, restored, 0},
    {"nif_open_directory", 2, open_directory, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_read_directory", 2, read_directory, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_close_directory", 1, close_directory, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_suppress_stdin_echo", 0, suppress_stdin_echo, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_restore_terminal", 1, restore_terminal, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_pager_start", 2, pager_start, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_pager_poll", 1, pager_poll, 0},
    {"nif_pager_close", 1, pager_close, 0}
};
ERL_NIF_INIT(Elixir.ExStorageServiceCli.CLI.Signals.Native, functions, load, NULL, NULL, NULL)
