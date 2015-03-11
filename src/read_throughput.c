/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   test_cuckoo.c
 * @author Bin Fan <binfan@cs.cmu.edu>
 * @date   Thu Feb 28 15:54:47 2013
 * 
 * @brief  a simple example of using cuckoo hash table with multiple threads
 * 
 * 
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include <unistd.h>           /* for sleep */
#include <sys/time.h>        /* for gettimeofday */
#ifdef HAVE_GETOPT_H
#  include <getopt.h>
#endif

#include "cuckoohash.h"


// The number of keys to size the table with, expressed as a power of 2
size_t power = 25;
// The number of threads spawned for inserts.
size_t thread_num;
// The load factor to fill the table up to before testing throughput.
size_t load = 90;
// The seed which the random number generator uses.
size_t seed = 0;
// How many seconds to run the test for.
size_t test_len = 10;

/* TODO(awreece) Get this from automake. */
#define CACHE_LINE_SIZE 64

static volatile bool keep_reading = true;

typedef struct {
    size_t numkeys;
    KeyType* keys;
    cuckoo_hashtable_t* table;
    size_t init_size;
} ReadEnvironment;

typedef struct {
    ReadEnvironment* env;
    size_t start, end;
    size_t numreads;
    int success;
    int in_table;
} __attribute__((aligned (CACHE_LINE_SIZE) )) read_thread_arg_t;


static void *read_thread(void* arg) {
    read_thread_arg_t* args = (read_thread_arg_t*)arg;
    args->success = 1;
    args->numreads = 0;
    ValType v;
    cuckoo_status st;
    while (keep_reading) {
        for (size_t i = args->start; i < args->end; i++) {
            st = cuckoo_find(
                args->env->table, (const char*) (&args->env->keys[i]),
                (char*) (&v));
            if (args->in_table) {
                if (st != ok) {
                    printf("Failed in read %zu\n", i);
                    args->success = 0;
                    break;
                }
            } else {
                if (st == ok) {
                    printf("Failed out read %zu\n", i);
                    args->success = 0;
                    break;
                }
            }
            ++args->numreads;
        }
        if (!args->success) {
            break;
        }
    }
    pthread_exit(NULL);
}

ReadEnvironment* CreateReadEnvironment() {
    ReadEnvironment *env = malloc(sizeof(ReadEnvironment));
    if (env == NULL) {
        goto Create_free_and_exit;
    }

    env->numkeys = 1U << power;
    env->table = cuckoo_init(power);
    env->keys = malloc(env->numkeys * sizeof(KeyType));
    if (env->table == NULL || env->keys == NULL) {
        goto Create_free_and_exit;
    }

    if (seed == 0) {
        srand(time(NULL));
    } else {
        srand(seed);
    }

    // We fill the keys array with integers between numkeys and 2*numkeys,
    // shuffled randomly
    env->keys[0] = env->numkeys + 1;
    for (size_t i = 1; i < env->numkeys; i++) {
        const size_t swapind = rand() % i;
        env->keys[i] = env->keys[swapind];
        env->keys[swapind] = (KeyType)(i + 1);
    }

    // We prefill the table to load with 1 threads. Since inserts are
    // serialized, 1 thread will be the fastest
    size_t total_keys = env->numkeys * (load / 100.0);
    for (size_t i = 0; i < total_keys; i++) {
        ValType v = 0;
        cuckoo_status st = cuckoo_insert(
            env->table, (const char*) &env->keys[i],
            (const char*) &v);
        if (st != ok) {
            fprintf(stderr, "Insert thread failed\n");
            break;
        }
    }
    env->init_size = total_keys;
    printf("Table with capacity %zu prefilled to a load factor of %zu%%\n",
           env->numkeys, load);
    goto Create_end;

Create_free_and_exit:
    if (env != NULL) {
        if (env->keys != NULL) {
            free(env->keys);
        }
        if (env->table != NULL) {
            cuckoo_exit(env->table);
        }
        free(env);
    }
    exit(EXIT_FAILURE);
Create_end:
    return env;
}

int main(int argc, char** argv) {
    thread_num = sysconf(_SC_NPROCESSORS_ONLN);
    int success = 1;
    ReadEnvironment* env = CreateReadEnvironment();
    // We use the first chunk of the threads to read the init_size elements
    // that are in the table and the others to read the numkeys-init_size
    // elements that aren't in the table. We proportion the number of threads
    // based on the load factor.
    const size_t first_threadnum = thread_num * (load / 100.0);
    const size_t second_threadnum = thread_num - first_threadnum;
    const size_t in_keys_per_thread = (first_threadnum == 0) ?
        0 : env->init_size / first_threadnum;
    const size_t out_keys_per_thread = (env->numkeys - env->init_size) /
        second_threadnum;
    read_thread_arg_t* read_args = malloc(
        thread_num * sizeof(read_thread_arg_t));
    pthread_t* readers = malloc(thread_num * sizeof(pthread_t));
    if (read_args == NULL || readers == NULL) {
        goto main_free_and_exit;
    }
    size_t i;
    for (i = 0; i < first_threadnum; i++) {
        read_args[i].env = env;
        read_args[i].start = i*in_keys_per_thread;
        read_args[i].end = (i+1)*in_keys_per_thread;
        read_args[i].in_table = 1;
        if (pthread_create(&readers[i], NULL, read_thread,
                           &read_args[i]) != 0) {
            fprintf(stderr, "Failed to create read thread\n");
            goto main_free_and_exit;
        }
    }
    for (; i < thread_num; i++) {
        int mul_ind = i - first_threadnum;
        read_args[i].env = env;
        read_args[i].start = mul_ind*out_keys_per_thread + env->init_size;
        read_args[i].end = (mul_ind+1)*out_keys_per_thread + env->init_size;
        read_args[i].in_table = 0;
        if (pthread_create(&readers[i], NULL, read_thread,
                           &read_args[i]) != 0) {
            fprintf(stderr, "Failed to create read thread\n");
            goto main_free_and_exit;
        }
    }


    sleep(test_len);
    keep_reading = false;

    for (size_t i = 0; i < thread_num; i++) {
        pthread_join(readers[i], NULL);
        if (!read_args[i].success) {
            fprintf(stderr, "Read thread %zu failed\n", i);
        }
    }

    size_t total_reads = 0;
    for (size_t i = 0; i < thread_num; i++) {
        total_reads += read_args[i].numreads;
    }
    // Reports the results
    printf("----------Results----------\n");
    printf("Number of reads:\t%zu\n", total_reads);
    printf("Time elapsed:\t%zu seconds\n", test_len);
    printf("Throughput: %f reads/sec\n", total_reads / (double)test_len);


main_free_and_exit:
    if (env != NULL) {
        free(env->keys);
        cuckoo_exit(env->table);
        free(env);
    } else {
        success = 0;
    }

    if (read_args != NULL) {
        free(read_args);
    } else {
        success = 0;
    }

    if (readers != NULL) {
        free(readers);
    } else {
        success = 0;
    }

    exit(success ? EXIT_SUCCESS : EXIT_FAILURE);
}
