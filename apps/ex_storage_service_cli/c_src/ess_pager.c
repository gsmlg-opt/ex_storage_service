/* Literal-argv pager runner. FD3 is the BEAM lifetime/cancellation pipe. */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 3) return 125;
    int input = open(argv[1], O_RDONLY | O_NOFOLLOW);
    struct stat st;
    if (input < 0 || fstat(input, &st) != 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid()) return 125;
    int tty = open("/dev/tty", O_RDWR | O_NOCTTY);
    pid_t foreground = tty >= 0 ? tcgetpgrp(tty) : -1;
    struct termios original;
    int saved_terminal = tty >= 0 && tcgetattr(tty, &original) == 0;
    struct sigaction ignore = {0};
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);
    (void)sigaction(SIGTTOU, &ignore, NULL);
    (void)sigaction(SIGINT, &ignore, NULL);
    (void)sigaction(SIGTERM, &ignore, NULL);
    int gate[2];
    if (pipe(gate) != 0) return 125;
    pid_t child = fork();
    if (child < 0) return 125;
    if (child == 0) {
        close(gate[1]);
        (void)setpgid(0, 0);
        char ready;
        if (read(gate[0], &ready, 1) != 1) _exit(125);
        close(gate[0]);
        struct sigaction normal = {0};
        normal.sa_handler = SIG_DFL;
        sigemptyset(&normal.sa_mask);
        (void)sigaction(SIGINT, &normal, NULL);
        (void)sigaction(SIGTERM, &normal, NULL);
        (void)sigaction(SIGTTOU, &normal, NULL);
        (void)sigaction(SIGTTIN, &normal, NULL);
        if (dup2(input, STDIN_FILENO) < 0) _exit(125);
        if (input > 2) close(input);
        close(3); close(4);
        if (tty >= 0) close(tty);
        execv(argv[2], &argv[2]);
        _exit(127);
    }
    close(gate[0]); close(input);
    (void)setpgid(child, child);
    if (foreground >= 0 && tcsetpgrp(tty, child) != 0) {
        (void)kill(-child, SIGKILL);
        close(gate[1]);
        (void)waitpid(child, NULL, 0);
        close(tty);
        return 125;
    }
    (void)write(gate[1], "R", 1);
    close(gate[1]);
    int status = 0, cancelled = 0;
    for (;;) {
        pid_t done = waitpid(child, &status, WNOHANG);
        if (done == child) break;
        if (done < 0 && errno != EINTR) { cancelled = 1; break; }
        struct pollfd control = { .fd = 3, .events = POLLIN | POLLHUP | POLLERR };
        int result = poll(&control, 1, 20);
        if (result > 0 && control.revents != 0) {
            cancelled = 1;
            (void)kill(-child, SIGKILL);
            while (waitpid(child, &status, 0) < 0 && errno == EINTR) {}
            break;
        }
    }
    /* Also reclaim descendants when a pager exits before its children. */
    (void)kill(-child, SIGKILL);
    if (foreground >= 0) (void)tcsetpgrp(tty, foreground);
    if (saved_terminal) (void)tcsetattr(tty, TCSANOW, &original);
    if (tty >= 0) close(tty);
    int result = cancelled ? 130 : WIFEXITED(status) ? WEXITSTATUS(status) :
        WIFSIGNALED(status) ? 128 + WTERMSIG(status) : 125;
    /* Completion is published only after foreground and termios restoration. */
    (void)write(4, &result, sizeof(result));
    return result;
}
