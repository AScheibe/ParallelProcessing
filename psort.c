#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
pthread_mutex_t lock;

typedef struct _thread_data_t {
    int tid;
    int low;
    int high;
    int num_lines;
    char** lines;
} thread_data_t;


typedef struct _chunk {
    int low;
    int high;
    struct _chunk* next;
} chunk_t;

int keycmp(const void* a, const void* b) {
    return *(int*)a - *(int*)b;
}


void merging(int low, int mid, int high, char** lines, char** lines_ph) {
    int l1, l2, i;

    for(l1 = low, l2 = mid + 1, i = low; l1 <= mid && l2 <= high; i++) {
        if(keycmp(lines[l1], lines[l2]) <= 0)
            lines_ph[i] = lines[l1++];
        else
            lines_ph[i] = lines[l2++];
    }

    while(l1 <= mid)
        lines_ph[i++] = lines[l1++];

    while(l2 <= high)
        lines_ph[i++] = lines[l2++];

   for(i = low; i <= high; i++)
      lines[i] = lines_ph[i];
}

// low, high inclusive
void sort(int low, int high, char** lines, char** lines_ph) {
   int mid;

   if(low < high) {
      mid = (low + high) / 2;
      sort(low, mid, lines, lines_ph);
      sort(mid+1, high, lines, lines_ph);
      merging(low, mid, high, lines, lines_ph);
   } else {
      return;
   }
}

void *sort_thread(void* thr_data) {
    thread_data_t *data = (thread_data_t*) thr_data;

    char** lines_ph = malloc(data->num_lines * sizeof(char*));
    for(int i = 0; i < data->num_lines; i++){
        lines_ph[i] = malloc(100 * sizeof(char));
    }

    sort(data->low, data->high, data->lines, lines_ph);

    // for (int i = 0; i < data->num_lines; i++) {
    //     free(lines_ph[i]);
    // }
}

int parallel_sort(char** lines, int total_lines, int num_threads){
    pthread_t threads[num_threads];
    thread_data_t thr_data[num_threads];

    if (lines == NULL) {
        return 1;
    }

    int rc;

    int chunk_size = 0;

    if(num_threads == 0 || (chunk_size = total_lines/num_threads) == 0){
        char** lines_ph = malloc(total_lines * sizeof(char*));
        for(int i = 0; i < total_lines; i++){
            lines_ph[i] = malloc(100 * sizeof(char));
        }

        sort(0, total_lines - 1, lines, lines_ph);

        // for(int i = 0; i < total_lines; i++) {
        //     free(lines_ph[i]);
        // }
        free(lines_ph);
    } else {
        /* create threads */
        for (int i = 0; i < num_threads; ++i) {
            thr_data[i].tid = i;
            thr_data[i].lines = lines;
            thr_data[i].low = i * chunk_size;
            if (i == num_threads - 1) {
                thr_data[i].high = total_lines - 1; // high index inclusive
            } else {
                thr_data[i].high = (i + 1) * chunk_size - 1; // one below start of next chunk
            }
            thr_data[i].num_lines = total_lines;

            if ((rc = pthread_create(&threads[i], NULL, sort_thread, &thr_data[i]))) {
                fprintf(stderr, "error: pthread_create, rc: %d\n", rc);
                return -1;
            }
        }

        /* block until all threads complete */
        for (int i = 0; i < num_threads; ++i) {
            pthread_join(threads[i], NULL);
        }

        char** lines_ph = malloc(total_lines * sizeof(char*));
        for(int i = 0; i < total_lines; i++){
            lines_ph[i] = malloc(100 * sizeof(char));
        }

      
    }
    return 0;
}

int main(int argc, char const *argv[])
{
    if (pthread_mutex_init(&lock, NULL) != 0) {
        printf("\n mutex init has failed\n");
        return 1;
    }

    if (argc != 4) {
        perror("Invalid args!\n");
        exit(EXIT_FAILURE);
    }

    // read in args
    FILE *fp_in = fopen(argv[1], "r");
    FILE *fp_out = fopen(argv[2], "w+");

    if (fp_in == NULL || fp_out == NULL) {
        perror("file failed to open");
        exit(EXIT_FAILURE);
    }

    int num_threads = atoi(argv[3]);

    // get number of keys/lines in file
    int num_lines = 0;
    int ch = 0;

    while(!feof(fp_in))
    {
        ch = fgetc(fp_in);
        if(ch == '\n')
        {
            num_lines++;
        }
    }

    // allocate array of lines in file
    char** lines = malloc(num_lines * sizeof(char*));
    for(int i = 0; i < num_lines; i++){
        lines[i] = malloc(100 * sizeof(char));
    }

    // copy lines in file to array
    size_t len = 0;
    char* line = NULL;
    int i = 0;
    ssize_t nread = 0;

    fclose(fp_in);

    fp_in = fopen(argv[1], "r");

    while ((nread = getline(&line, &len, fp_in)) != -1 &&
            i < num_lines) {
        strncpy(lines[i], line, 100);
        i++;
    }

    free(line);

    int retval = 0;

    // sort lines and time
    clock_t start = clock();
    clock_t diff;

    int psort_rc = parallel_sort(lines, num_lines, num_threads);
    diff = clock() - start;

    if (psort_rc == 0) {
        printf("psort success! Elapsed CPU time: %ld ms\n", diff * 1000 / CLOCKS_PER_SEC);
        // print to output file
        for (int i = 0; i < num_lines; i++){
            fprintf(fp_out, "%s", lines[i]);
        }

        fsync(fileno(fp_out));
        retval = 1;
    }


    fclose(fp_in);
    fclose(fp_out);

    for (int i = 0; i < num_lines; i++) {
        free(lines[i]);
    }
    pthread_mutex_destroy(&lock);
    free(lines);
    return retval;
}
