#define _GNU_SOURCE
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "threadpool.h"

const int NUMBER = 2;  // 添加线程一次加两个

// 任务结构体
typedef struct Task
{
	void (* function)(void* arg);
	void* arg;
}Task;

// 线程池结构体
struct ThreadPool
{
	// 任务队列
	Task* taskQ;
	int queueCapacity;  // 容量
	int queueSize;      // 当前任务个数
	int queueFront;     // 队头 -> 取数据
	int queueRear;		// 队尾 -> 存数据

	pthread_t managerID;        // 管理者线程ID
	pthread_t* threadIDs;		// 工作线程ID
	int minNum;					// 最小线程数量
	int maxNum;					// 最大线程数量
	int busyNum;				// 忙的线程数量
	int liveNum;				// 存活的线程数量
	int exitNum;                // 要销毁的线程数量
	pthread_mutex_t mutexPool;  // 锁整个线程池
	pthread_mutex_t mutexBusy;  // 锁busyNum变量(由于该变量使用频繁，每次操作都锁整个线程池会影响效率，因此专门用一把锁)
	pthread_cond_t notFull;     // 任务队列是否满了
	pthread_cond_t notEmpty;    // 任务队列是否空着

	int shutdown;           // 是不是要销毁线程池, 销毁为1, 不销毁为0
};

ThreadPool* threadPoolCreate(int min, int max, int queueCapacity)  // 最小线程数、最大线程数、任务队列容量
{
	// 初始化线程池结构体
	ThreadPool* pool = (ThreadPool*)malloc(sizeof(ThreadPool));

	do 
	{
		if (pool == NULL)
		{
			printf("malloc threadpool init fail...\n");
			break;
		}

		// 初始化工作线程
		pool->threadIDs = (pthread_t*)malloc(max * sizeof(pthread_t));
		if (pool->threadIDs == NULL)
		{
			printf("malloc threadIDs init fail...\n");
			break;
		}
		memset(pool->threadIDs, 0, max * sizeof(pthread_t));  // 如果线程ID为0则未被占用

		// 初始化线程池成员
		pool->minNum = min;
		pool->maxNum = max;
		pool->busyNum = 0;
		pool->liveNum = min;  // 和最小个数相等
		pool->exitNum = 0;

		if (pthread_mutex_init(&pool->mutexPool, NULL) != 0 ||
			pthread_mutex_init(&pool->mutexBusy, NULL) != 0 ||
			pthread_cond_init(&pool->notFull, NULL) != 0 ||
			pthread_cond_init(&pool->notEmpty, NULL) != 0)
		{
			printf("mutex or condition init fail...\n");
			break;
		}

		// 任务队列
		pool->taskQ = (Task*)malloc(queueCapacity * sizeof(Task));
		pool->queueCapacity = queueCapacity;
		pool->queueFront = 0;
		pool->queueRear = 0;

		pool->shutdown = 0;

		// 创建管理者线程和工作线程
		pthread_create(&pool->managerID, NULL, manager, pool);
		for (int i = 0; i < min; i++)
		{
			pthread_create(&pool->threadIDs[i], NULL, worker, pool);
		}

		return pool;
	} while (0);
	

	// 初始化失败就释放资源
	if (pool && pool->threadIDs)free(pool->threadIDs);
	if (pool && pool->taskQ)free(pool->taskQ);
	if (pool)free(pool);

	return NULL;
}

int threadPoolDestroy(ThreadPool* pool)
{
	if (pool == NULL)
	{
		return -1;
	}

	// 关闭线程池
	pool->shutdown = 1;

	// 阻塞回收管理者线程
	pthread_join(pool->managerID, NULL);

	// 唤醒阻塞的消费者线程(上面已经事先修改shutdown为1)，即回收消费者线程
	for (int i = 0; i < pool->liveNum; i++)
	{
		pthread_cond_signal(&pool->notEmpty);
	}

	// 等待回收消费者线程
	for (int i = 0; i < pool->maxNum; i++)
	{
		if (pool->threadIDs[i] != 0)
		{
			pthread_join(pool->threadIDs[i], NULL);
		}
	}

	// 回收互斥锁资源
	pthread_mutex_destroy(&pool->mutexBusy);
	pthread_mutex_destroy(&pool->mutexPool);

	// 回收条件变量
	pthread_cond_destroy(&pool->notEmpty);
	pthread_cond_destroy(&pool->notFull);

	// 回收堆内存
	if (pool->taskQ)
	{
		free(pool->taskQ);
		pool->taskQ = NULL;
	}
	if (pool->threadIDs)
	{
		free(pool->threadIDs);
		pool->threadIDs = NULL;
	}
	free(pool);
	pool = NULL;

	return 0;
}

void threadPoolAdd(ThreadPool* pool, void(*func)(void*), void* arg)
{
	pthread_mutex_lock(&pool->mutexPool);

	// 还有空间可以生产，并且线程池不关闭
	while (pool->queueSize == pool->queueCapacity && pool->shutdown == 0)
	{
		// 阻塞生产者线程
		pthread_cond_wait(&pool->notFull, &pool->mutexPool);
	}

	// 判断是否要销毁线程池
	if (pool->shutdown)
	{
		pthread_mutex_unlock(&pool->mutexPool);
		return;
	}

	// 添加任务
	pool->taskQ[pool->queueRear].function = func;
	pool->taskQ[pool->queueRear].arg = arg;
	pool->queueSize++;
	// 移动尾节点
	pool->queueRear = (pool->queueRear + 1) % pool->queueCapacity;

	// 唤醒消费者线程
	pthread_cond_signal(&pool->notEmpty);

	pthread_mutex_unlock(&pool->mutexPool);
}

int threadPoolBusyNum(ThreadPool* pool)
{
	pthread_mutex_lock(&pool->mutexBusy);
	int busyNum = pool->busyNum;
	pthread_mutex_unlock(&pool->mutexBusy);
	return busyNum;
}

int threadPoolAliveNum(ThreadPool* pool)
{
	pthread_mutex_lock(&pool->mutexPool);
	int aliveNum = pool->liveNum;
	pthread_mutex_unlock(&pool->mutexPool);
	return aliveNum;
}

void* worker(void* arg)
{
	ThreadPool* pool = (ThreadPool*)arg;

	while (1)
	{
		// 上互斥锁(获取任务)
		pthread_mutex_lock(&pool->mutexPool);

		// 当前任务队列为空，并且线程池未关闭
		while (pool->queueSize == 0 && pool->shutdown == 0)  // 这里的循环是防止虚假唤醒
		{
			// 阻塞工作线程
			pthread_cond_wait(&pool->notEmpty, &pool->mutexPool);

			// 判断是否要销毁该线程
			if (pool->exitNum > 0)
			{
				// 这步不放在下面的if是防止管理者一开始要销毁，但是生产者又发布了新的任务，此时执行销毁的非预期行为
				// 因此无论是否销毁都减pool->exitNum的值
				pool->exitNum--;  

				if (pool->liveNum > pool->minNum)
				{
					pool->liveNum--;
					pthread_mutex_unlock(&pool->mutexPool);
					threadExit(pool);
				}
			}
		}

		// 判断线程池是否被关闭了
		if (pool->shutdown && pool->queueSize == 0)
		{
			pthread_mutex_unlock(&pool->mutexPool);
			pthread_exit(NULL);
		}

		// 从任务队列中取出一个任务
		Task task;
		task.function = pool->taskQ[pool->queueFront].function;
		task.arg = pool->taskQ[pool->queueFront].arg;
		// 移动头结点
		pool->queueFront = (pool->queueFront + 1) % pool->queueCapacity;
		pool->queueSize--;

		// 唤醒生产者
		pthread_cond_signal(&pool->notFull);

		// 解互斥锁(结束获取任务)
		pthread_mutex_unlock(&pool->mutexPool);

		printf("thread %ld start working...\n", pthread_self());
		// 上互斥锁(修改忙的线程数量)
		pthread_mutex_lock(&pool->mutexBusy);
		pool->busyNum++;
		pthread_mutex_unlock(&pool->mutexBusy);

		// 执行任务
		task.function(task.arg);
		free(task.arg);
		task.arg = NULL;

		printf("thread %ld end working...\n", pthread_self());
		// 上互斥锁(修改忙的线程数量)
		pthread_mutex_lock(&pool->mutexBusy);
		pool->busyNum--;
		// 解互斥锁(结束修改)
		pthread_mutex_unlock(&pool->mutexBusy);
	}

	return NULL;
}

void* manager(void* arg)
{
	ThreadPool* pool = (ThreadPool*)arg;

	while (pool->shutdown == 0)
	{
		// 每隔3s检测一次
		sleep(3);

		// 上互斥锁(获取存活线程数量、最大线程数量、最小线程数量和当前任务个数)
		pthread_mutex_lock(&pool->mutexPool);
		int liveNum = pool->liveNum;
		int maxNum = pool->maxNum;
		int minNum = pool->minNum;
		int queueSize = pool->queueSize;
		pthread_mutex_unlock(&pool->mutexPool);

		// 上互斥锁(获取忙的线程数量)
		pthread_mutex_lock(&pool->mutexBusy);
		int busyNum = pool->busyNum;
		pthread_mutex_unlock(&pool->mutexBusy);

		// 添加线程 (存活线程数量 < 当前任务的个数 && 存活线程数量 < 最大线程数量)
		if (liveNum < queueSize && liveNum < maxNum)
		{
			int counter = 0;

			// 上互斥锁(开始添加线程)
			pthread_mutex_lock(&pool->mutexPool);
			// 遍历所有线程，挑出两个目前还未激活的线程
			for (int i = 0; i < maxNum && counter < NUMBER && liveNum < maxNum; i++)
			{
				if (pool->threadIDs[i] == 0)
				{
					pthread_create(&pool->threadIDs[i], NULL, worker, pool);  // 传入pool地址
					counter++;

					pool->liveNum++;  // 存活线程数量+1
					liveNum = pool->liveNum;
				}
			}
			pthread_mutex_unlock(&pool->mutexPool);
		}

		// 销毁线程 (忙的线程数量 * 2 < 存活的线程数量 && 存活的线程数量 > 最小线程数)
		if (busyNum * 2 < liveNum && liveNum > minNum)
		{
			// 上互斥锁(标志线程池处于需要销毁线程状态)
			pthread_mutex_lock(&pool->mutexPool);
			pool->exitNum = NUMBER;  // 一次销毁两个线程
			pthread_mutex_unlock(&pool->mutexPool);

			for (int i = 0; i < NUMBER; i++)
			{
				pthread_cond_signal(&pool->notEmpty);  // 让多余的子线程自己销毁
			}
		}
	}

	return NULL;
}

void threadExit(ThreadPool* pool)
{
	pthread_mutex_lock(&pool->mutexPool);
	pthread_t tid = pthread_self();
	for (int i = 0; i < pool->maxNum; i++)
	{
		if (pthread_equal(tid, pool->threadIDs[i]) != 0)  // 确保线程处于存活状态
		{
			pool->threadIDs[i] = 0;
			printf("threadExit() called, %ld exiting...\n", tid);
			break;
		}
	}
	pthread_mutex_unlock(&pool->mutexPool);

	pthread_exit(NULL);
}
