#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>

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


void merging(int low, int mid, int high, char** lines, int num_lines) {
    char* lines_ph2[num_lines];

    int l1, l2, i;

    for(l1 = low, l2 = mid + 1, i = low; l1 <= mid && l2 <= high; i++) {
        if(keycmp(lines[l1], lines[l2]) <= 0)
            lines_ph2[i] = lines[l1++];
        else
            lines_ph2[i] = lines[l2++];
    }

    while(l1 <= mid)
        lines_ph2[i++] = lines[l1++];

    while(l2 <= high)
        lines_ph2[i++] = lines[l2++];

    for(i = low; i <= high; i++)
        lines[i] = lines_ph2[i];
}

// low, high inclusive
void sort(int low, int high, char** lines, int num_lines) {
   int mid;

   if(low < high) {
      mid = (low + high) / 2;
      sort(low, mid, lines, num_lines);
      sort(mid+1, high, lines, num_lines);
      merging(low, mid, high, lines, num_lines);
   } else {
      return;
   }
}

void *sort_thread(void* thr_data) {
    thread_data_t *data = (thread_data_t*) thr_data;

    sort(data->low, data->high, data->lines, data->num_lines);

    return NULL;
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
        sort(0, total_lines - 1, lines, total_lines);
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
        
        chunk_t* chunk_head = NULL;
        for (int c = num_threads - 1; c >= 0; c--) {
            chunk_t* new_node = malloc(sizeof(chunk_t));
            new_node->low = thr_data[c].low;
            new_node->high = thr_data[c].high;
            new_node->next = chunk_head;
            chunk_head = new_node;
        }

        int num_chunks = num_threads;
        while (num_chunks > 1) {
            // [0 1 2 3 4]
            // chunks = 5
            // 0+1; 2+3; 4
            // chunks = 3
            // 01+23; 4
            // chunks = 2
            // 0123+4
            // chunks = 1
            // 01234 -> done!

            for(chunk_t* curr = chunk_head; curr->next != NULL; curr = curr->next) {
                int low = curr->low;
                int mid = curr->next->low - 1; // one below start of next chunk
                int high = curr->next->high; // top of next chunk

                merging(low, mid, high, lines, total_lines);
                curr->high = high;
                chunk_t* old = curr->next;
                curr->next = curr->next->next;
                free(old);

                num_chunks--;

                if(curr->next == NULL){
                    break;
                }
            }
        }
    }
    return 0;
}

int main(int argc, char const *argv[])
{
    struct stat st;
    struct timeval start_time, end_time;

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

    // get num_lines in O(1) -- file size always multiple of 100
    stat(argv[1], &st);
    num_lines = st.st_size / 100;

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

    gettimeofday(&start_time, NULL);
    int psort_rc = parallel_sort(lines, num_lines, num_threads);
    gettimeofday(&end_time, NULL);
    long seconds = end_time.tv_sec - start_time.tv_sec;
    long microseconds = end_time.tv_usec - start_time.tv_usec;
    double elapsed_time = seconds + (microseconds / 1000000.0);


    if (psort_rc == 0) {
        printf("Elapsed time: %f seconds\n", elapsed_time);

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

    free(lines);

    return retval;
}
