#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

typedef struct _thread_data_t {
    int tid;
    int low;
    int high;
    int num_lines;
    char** lines;
} thread_data_t;

int keycmp(char* line1, char* line2){
    char key1[4];
    char key2[4];

    strncpy(key1, line1, 4);
    strncpy(key2, line2, 4);

    return strcmp(key1, key2);
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
    printf("[sort_thread]\ttid: %d\tlow: %d\thigh: %d\n", data->tid, data->low, data->high);

    char** lines_ph = malloc(data->num_lines * sizeof(char*));
    for(int i = 0; i < data->num_lines; i++){
        lines_ph[i] = malloc(100 * sizeof(char));
    }

    sort(data->low, data->high, data->lines, lines_ph);

    free(lines_ph);

    pthread_exit(NULL);
}

int parallel_sort(char** lines, int num_lines, int num_threads){
    pthread_t threads[num_threads];
    thread_data_t thr_data[num_threads];

    if (lines == NULL) {
        return 1;
    }

    int rc;

    int chunk_size = 0;

    if(num_threads == 0 || (chunk_size = num_lines/num_threads) == 0){
        char** lines_ph = malloc(num_lines * sizeof(char*));
        for(int i = 0; i < num_lines; i++){
            lines_ph[i] = malloc(100 * sizeof(char));
        }

        sort(0, num_lines - 1, lines, lines_ph);
    } else {
        /* create threads */
        for (int i = 0; i < num_threads; ++i) {
            thr_data[i].tid = i;
            thr_data[i].lines = lines;
            thr_data[i].low = i * chunk_size;
            if (i == num_threads - 1) {
                thr_data[i].high = num_lines - 1; // high index inclusive
            } else {
                thr_data[i].high = (i + 1) * chunk_size - 1; // one below start of next chunk
            }

            if ((rc = pthread_create(&threads[i], NULL, sort_thread, &thr_data[i]))) {
                fprintf(stderr, "error: pthread_create, rc: %d\n", rc);
                return -1;
            }
        }

        /* block until all threads complete */
        for (int i = 0; i < num_threads; ++i) {
            pthread_join(threads[i], NULL);
        }

        char** lines_ph = malloc(num_lines * sizeof(char*));
        for(int i = 0; i < num_lines; i++){
            lines_ph[i] = malloc(100 * sizeof(char));
        }

        int low = 0;
        int high = 0;

        for(int i = 0; i < num_threads; i += 2){
            int mid = ((i + 1) * chunk_size) - 1; // one below start of next chunk

            if (i == num_threads) {
                high = num_lines - 1; // high index inclusive
            } else {
                high = (i + 2) * chunk_size - 1; // one below start of next chunk
            }


            merging(low, mid, high, lines, lines_ph);

            low = high;
        }

        free(lines_ph);
    }
    return 0;
}

int main(int argc, char const *argv[])
{
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
    printf("code was recompiled!");

    fclose(fp_in);

    FILE *fp_in = fopen(argv[1], "r");

    while ((nread = getline(&line, &len, fp_in)) != -1) {
        printf("getline: %s\n", line);
        strcpy(lines[i], line);
        i++;
    }

    free(line);



    int retval = 0;

    // sort lines
    if (parallel_sort(lines, num_lines, num_threads) == 0) {
        printf("psort success!\n");
        // print to output file
        for (int i = 0; i < num_lines; i++){
            fprintf(fp_out, "%s", lines[i]);
        }

        fsync(fileno(fp_out));
        retval = 1;
    }


    fclose(fp_in);
    fclose(fp_out);
    free(lines);
    return retval;
}
