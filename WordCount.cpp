


#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <keyvalue.h>
#include <mapreduce.h>


using namespace MAPREDUCE_NS;

void fileread(int, KeyValue *, void *);
void sum(char *, int, char *, int, int *, KeyValue *, void *);
int ncompare(char *, int, char *, int);
void output(int, char *, int, char *, int, KeyValue *, void *);

struct Count {
  int n,lim,flag;
};



int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int node;
  int Nprocs;
  
  MPI_Comm_rank(MPI_COMM_WORLD,&node);
  
  MPI_Comm_size(MPI_COMM_WORLD,&Nprocs);

  if (narg <= 1) {
    if (node == 0) printf("Word frequency file1 file2 ...\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD);
  
  double tstart = MPI_Wtime();

  int theWords = mr->map(narg-1,&fileread,&args[1]);
  
  mr->collate(NULL);
  
  int nU = mr->reduce(&sum,NULL);

  MPI_Barrier(MPI_COMM_WORLD);
  
  double tstop = MPI_Wtime();

  mr->sort_values(&ncompare);

  Count count;
  count.n = 0;
  count.lim = 10;
  count.flag = 0;
  mr->map(mr->kv,&output,&count);
  
  mr->gather(1);
  mr->sort_values(&ncompare);

  count.n = 0;
  count.lim = 10;
  count.flag = 1;
  mr->map(mr->kv,&output,&count);

  delete mr;

  if (node == 0) {
    printf("%d total words, %d unique words\n",Nwords,nU);
    printf("Time to process %d files on %d procs = %g (secs)\n",
     narg-1,Nprocs,tstop-tstart);
  }

  MPI_Finalize();
}



void fileread(int theTask, KeyValue *kv, void *ptr)
{
  

  char **files = (char **) ptr;

  struct stat stbuf;
  int flag = stat(files[theTask],&stbuf);
  if (flag < 0) {
    printf("ERR\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int fileS = stbuf.st_size;

  FILE *fp = fopen(files[theTask],"r");
  char *text = new char[fileS+1];
  int nchar = fread(text,1,fileS,fp);
  text[nchar] = '\0';
 
  fclose(fp);

  char *space = " \t\n\f\r\0";
 
  char *word = strtok(text,space);
  
  while (word) {
   
    kv->add(word,strlen(word)+1,NULL,0);
   
    word = strtok(NULL,space);
  }

  delete [] text;
}


void sum(char *key, int keyB, char *multivalue,
   int nvalues, int *valB, KeyValue *kv, void *ptr) 
{
  kv->add(key,keyB,(char *) &nvalues,sizeof(int));
}



int ncompare(char *p1, int len1, char *p2, int len2)
{
  int i1 = *(int *) p1;
  int i2 = *(int *) p2;
  if (i1 > i2) return -1;
  else if (i1 < i2) return 1;
  else return 0;
}



void output(int theTask, char *key, int keyB, char *value,
      int valB, KeyValue *kv, void *ptr)
{
  Count *count = (Count *) ptr;
  count->n++;
  if (count->n > count->lim) return;

  int n = *(int *) value;
  if (count->flag) printf("%d %s\n",n,key);
  else kv->add(key,keyB,(char *) &n,sizeof(int));
}