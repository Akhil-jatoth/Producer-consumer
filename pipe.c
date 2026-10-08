// user/prodcons.c
// Producer-Consumer demo using an XV6 pipe.
//
// Usage (inside xv6 shell):
//   $ prodcons          -> runs all three tests
//   $ prodcons 1        -> test 1 only (pipe FULL: slow consumer)
//   $ prodcons 2        -> test 2 only (pipe EMPTY: slow producer)
//   $ prodcons 3        -> test 3 only (EOF when writer closes)

#include "kernel/types.h"
#include "kernel/stat.h"
#include "user/user.h"

#define TOTAL 2048        // total bytes sent through the pipe
#define PIPESIZE 512      // must match kernel/pipe.c

// Byte pattern so the consumer can verify data integrity.
static char pattern(int i) { return (char)('A' + (i % 26)); }

// Producer: writes TOTAL bytes in chunks of `chunk`,
// sleeping `delay` ticks after each write.
static void producer(int wfd, int chunk, int delay)
{
  char buf[256];
  int sent = 0;

  while (sent < TOTAL) {
    int n = chunk;
    if (sent + n > TOTAL)
      n = TOTAL - sent;
    for (int i = 0; i < n; i++)
      buf[i] = pattern(sent + i);

    int t0 = uptime();
    int w = write(wfd, buf, n);          // blocks if pipe is FULL
    int t1 = uptime();
    if (w != n) {
      printf("[producer] write failed (%d)\n", w);
      exit(1);
    }
    sent += w;
    printf("[producer] wrote %d bytes (total %d)  blocked for %d ticks%s\n",
           w, sent, t1 - t0, (t1 - t0) > 1 ? "  <-- pipe was FULL" : "");
    if (delay)
      sleep(delay);
  }
  printf("[producer] done, closing write end\n");
  close(wfd);                            // consumer will see EOF
}

// Consumer: reads until EOF, verifying the byte pattern.
static void consumer(int rfd, int chunk, int delay)
{
  char buf[256];
  int got = 0, errors = 0;

  for (;;) {
    int t0 = uptime();
    int r = read(rfd, buf, chunk);       // blocks if pipe is EMPTY
    int t1 = uptime();
    if (r < 0) {
      printf("[consumer] read error\n");
      exit(1);
    }
    if (r == 0)
      break;                             // EOF: writer closed and pipe drained
    for (int i = 0; i < r; i++)
      if (buf[i] != pattern(got + i))
        errors++;
    got += r;
    printf("[consumer] read  %d bytes (total %d)  blocked for %d ticks%s\n",
           r, got, t1 - t0, (t1 - t0) > 1 ? "  <-- pipe was EMPTY" : "");
    if (delay)
      sleep(delay);
  }
  printf("[consumer] EOF reached. total=%d bytes, corrupted=%d\n", got, errors);
  if (got != TOTAL || errors != 0) {
    printf("[consumer] FAILED\n");
    exit(1);
  }
  printf("[consumer] data verified OK\n");
  close(rfd);
}

// Runs one experiment: forks a consumer child; parent is the producer.
static void run_test(char *name, int pchunk, int pdelay, int cchunk, int cdelay)
{
  int p[2];

  printf("\n=== %s ===\n", name);
  if (pipe(p) < 0) {
    printf("pipe failed\n");
    exit(1);
  }

  int pid = fork();
  if (pid < 0) {
    printf("fork failed\n");
    exit(1);
  }

  if (pid == 0) {                        // child = CONSUMER
    close(p[1]);                         // close unused write end
    consumer(p[0], cchunk, cdelay);
    exit(0);
  }

  close(p[0]);                           // parent = PRODUCER, close unused read end
  producer(p[1], pchunk, pdelay);

  int status;
  wait(&status);
  printf("=== %s finished (consumer exit status %d) ===\n", name, status);
}

// Test 3: reader gets EOF (read returns 0) when all writers close,
// and writer gets an error when all readers close.
static void test_close_semantics(void)
{
  int p[2];
  char c = 'x';

  printf("\n=== Test 3: close semantics ===\n");

  // (a) writer closes -> read returns 0
  pipe(p);
  close(p[1]);
  int r = read(p[0], &c, 1);
  printf("read after writer closed returned %d (expected 0 = EOF)\n", r);
  close(p[0]);

  // (b) reader closes -> write returns -1
  pipe(p);
  close(p[0]);
  int w = write(p[1], &c, 1);
  printf("write after reader closed returned %d (expected -1)\n", w);
  close(p[1]);
}

int main(int argc, char *argv[])
{
  int which = 0;
  if (argc > 1)
    which = atoi(argv[1]);

  printf("Producer-Consumer on XV6 pipe (PIPESIZE=%d, TOTAL=%d)\n",
         PIPESIZE, TOTAL);

  // Test 1: FAST producer (128-byte chunks, no delay)
  //         SLOW consumer (64-byte chunks, 10-tick delay)
  //         -> pipe fills to 512 bytes, producer sleeps on &pi->nwrite.
  if (which == 0 || which == 1)
    run_test("Test 1: pipe FULL (slow consumer)", 128, 0, 64, 10);

  // Test 2: SLOW producer (64-byte chunks, 10-tick delay)
  //         FAST consumer (128-byte reads, no delay)
  //         -> pipe is empty most of the time, consumer sleeps on &pi->nread.
  if (which == 0 || which == 2)
    run_test("Test 2: pipe EMPTY (slow producer)", 64, 10, 128, 0);

  // Test 3: EOF / broken-pipe behaviour.
  if (which == 0 || which == 3)
    test_close_semantics();

  exit(0);
}