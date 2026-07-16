#include <stdio.h>
#include "threadpool.h"
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>

void taskFunc(void* arg)
{
	int num = *(int*)arg;
	printf("thread %ld is working, number = %d\n", pthread_self(), num);
	sleep(1);
}

int main()
{
	// 创建线程池，最小线程数为3，最大线程数为10，任务队列容量为100
	ThreadPool* pool = threadPoolCreate(3, 10, 100);  

	printf("666\n");

	for (int i = 0; i < 100; i++)
	{
		int* num = (int*)malloc(sizeof(int));
		*num = i + 100;
		threadPoolAdd(pool, taskFunc, num);
	}

	printf("666\n");

	// 销毁线程池
	threadPoolDestroy(pool);

	printf("666\n");

    return 0;
}
