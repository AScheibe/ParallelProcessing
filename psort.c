#include <stdio.h>
#include <string.h>


int keycmp(char* line1, char* line2){
    char* key1[4];
    char* key2[4];

    strncpy(key1, line1, 4);
    strncpy(key2, line2, 4);

    return strcmp(key1, key2);
}

void merging(int low, int mid, int high, char** lines, char** lines_ph) {
   int l1, l2, i;

   for(l1 = low, l2 = mid + 1, i = low; l1 <= mid && l2 <= high; i++) {
      if(keycmp(lines, lines_ph) <= 0)
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

void psort(FILE* fp_in, FILE* fp_out, int num_keys, int num_threads){

}




int main(int argc, char const *argv[])
{
    if(argc != 4){
        printf("Invalid Input");
        return;
    }

    // create file pointers
    FILE *fp_in = fopen(argv[1], "r");
    FILE *fp_out = fopen(argv[2], "w+");

    // get number of keys/lines in file
    int num_keys;
    int ch = 0;

    while(!feof(fp_in))
    {
        ch = fgetc(fp_in);
        if(ch == '\n')
        {
            num_keys++;
        }
    }

    // allocate array of lines in file
    char** lines = malloc(num_keys * sizeof(char*));
    for(int i = 0; i < num_keys; i++){
        lines[i] = malloc(100 * sizeof(char));
    }

    char** lines_ph = malloc(num_keys * sizeof(char*));

    for(int i = 0; i < num_keys; i++){
        lines_ph[i] = malloc(100 * sizeof(char));
    }

    // copy lines in file to array
    size_t len = 0;
    char* line;
    int i = 0;
    
    while(getline(&line, &len, fp_in) != -1){
        strcpy(lines[i], line);
    }    

    psort();

    fclose(fp_in);
    fclose(fp_out);
    free(lines);
    return 0;   
}