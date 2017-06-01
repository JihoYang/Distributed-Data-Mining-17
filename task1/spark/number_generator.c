#include <stdio.h> 
#include <stdlib.h>

int main(){
	unsigned long MAX_NUM = 10e4;
	FILE *fp=NULL; 
	fp = fopen("input_numbers.txt", "w");
	for (unsigned long i = 0; i < MAX_NUM; i++){
		fprintf(fp, "%lu ", i+1);
		printf("index = %lu\n", i);
	}
	return 0;
}
