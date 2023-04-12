#include <stdio.h>
#include <string.h>

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
    free(keys);
    return 0;   
}


int cmpKeys(char* line1, char* line2){
    char* key1[4];
    char* key2[4];

    strncpy(key1, line1, 4);
    strncpy(key2, line2, 4);

    return strcmp(key1, key2);
}

void psort(FILE* fp_in, FILE* fp_out, int num_keys, int num_threads){

}




