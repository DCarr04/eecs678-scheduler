/** @file libscheduler.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libscheduler.h"
#include "../libpriqueue/libpriqueue.h"


/**
  Stores information making up a job to be scheduled including any statistics.

  You may need to define some global variables or a struct to store your job queue elements. 
*/
typedef struct _job_t
{
  int jobID;
  int arrivalTime;
  int burstTime;
  int priority;
  int startTime;
  int completionTime;
  int remainingTime;
  int coreID;

} job_t;

static int coresNum;
static scheme_t schedScheme;
static priqueue_t jobQueue;
static int totalJobs;
static int jobsCapacity;
static job_t **trackJobs;
static job_t **coreJobs;

int comparerFCFS(const void *x, const void *y){
  job_t *x1 = (job_t *)x; //type cast const void into job_t
  job_t *y1 = (job_t *)y;
  
  return y1->arrivalTime - x1->arrivalTime;
}

int comparerSJF(const void *x, const void *y){
  job_t *x1 = (job_t *)x; //type cast const void into job_t
  job_t *y1 = (job_t *)y;

  if(y1->burstTime == x1->burstTime){
    return y1->arrivalTime - x1->arrivalTime;
  }else{
    return y1->burstTime - x1->burstTime;
  }
}

int comparerRR(const void *x, const void *y){
  return comparerFCFS(x, y);
}

int comparerPRI(const void *x, const void *y){
  job_t *x1 = (job_t *)x;
  job_t *y1 = (job_t *)y;
  if(y1->priority == x1->priority){
    return y1->arrivalTime - x1->arrivalTime;
  }else{
    return y1->priority - x1->priority;
  }
}

/**
  Initalizes the scheduler.
 
  Assumptions:
    - You may assume this will be the first scheduler function called.
    - You may assume this function will be called once once.
    - You may assume that cores is a positive, non-zero number.
    - You may assume that scheme is a valid scheduling scheme.

  @param cores the number of cores that is available by the scheduler. These cores will be known as core(id=0), core(id=1), ..., core(id=cores-1).
  @param scheme  the scheduling scheme that should be used. This value will be one of the six enum values of scheme_t
*/
void scheduler_start_up(int cores, scheme_t scheme)
{
  coresNum = cores;
  schedScheme = scheme;
  if(scheme == FCFS){
    priqueue_init(&jobQueue, comparerFCFS);
  }else if(scheme == SJF){
    priqueue_init(&jobQueue, comparerSJF);
  }else if(scheme == RR){
    priqueue_init(&jobQueue, comparerRR);
  }else if(scheme == PRI){
    priqueue_init(&jobQueue, comparerPRI);
  }else if(scheme == PPRI){
    priqueue_init(&jobQueue, comparerPRI);
  }else if(scheme == PSJF){
    priqueue_init(&jobQueue, comparerSJF);
  }

  jobsCapacity = 10;
  totalJobs = 0;
  trackJobs = malloc(sizeof(job_t *) * jobsCapacity);

  coreJobs = malloc(sizeof(job_t *) * coresNum);
  for(int i = 0; i < coresNum; i++){
    coreJobs[i] = NULL;
  } 
}


/**
  Called when a new job arrives.
 
  If multiple cores are idle, the job should be assigned to the core with the
  lowest id.
  If the job arriving should be scheduled to run during the next
  time cycle, return the zero-based index of the core the job should be
  scheduled on. If another job is already running on the core specified,
  this will preempt the currently running job.
  Assumption:
    - You may assume that every job wil have a unique arrival time.
 
  @param job_number a globally unique identification number of the job arriving.
  @param time the current time of the simulator.
  @param running_time the total number of time units this job will run before it will be finished.
  @param priority the priority of the job. (The lower the value, the higher the priority.)
  @return index of core job should be scheduled on
  @return -1 if no scheduling changes should be made. 
 
 */
int scheduler_new_job(int job_number, int time, int running_time, int priority)
{
  job_t *newJob = malloc(sizeof(job_t));
  newJob->jobID = job_number;
  newJob->arrivalTime = time;
  newJob->burstTime = running_time;
  newJob->remainingTime = running_time;
  newJob->priority = priority;
  newJob->startTime = -1;
  newJob->completionTime = -1;
  newJob->coreID = -1;

  if(totalJobs >= jobsCapacity){
    jobsCapacity = jobsCapacity * 2;
    trackJobs = realloc(trackJobs, sizeof(job_t *) * jobsCapacity);
  }
  trackJobs[totalJobs] = newJob;
  totalJobs++;

  for(int i = 0; i < coresNum; i++){
    if(coreJobs[i] == NULL){
      coreJobs[i] = newJob;
      newJob->startTime = time;
      newJob->coreID = i;
      return i;
    }
  }

  priqueue_offer(&jobQueue, newJob);

  if(schedScheme == SJF || schedScheme == PRI){
    int preemptCore = -1;

    for(int i = 0; i < coresNum; i++){
      
      if(coreJobs[i] != NULL){
        int compare = 0;
        int comparePreemptCore = 0;

        if(schedScheme == SJF){
          compare = comparerSJF(newJob, coreJobs[i]);
          comparePreemptCore = comparerSJF(coreJobs[i], coreJobs[preemptCore]);
        }else{
          compare = comparerPRI(newJob, coreJobs[i]);
          comparePreemptCore = comparerPRI(coreJobs[i], coreJobs[preemptCore]);
        }
        
        //(schedScheme == SJF ? comparerSJF(coreJobs[i], coreJobs[preemptCore]) > 0 : comparerPRI(coreJobs[i], coreJobs[preemptCore]) > 0)
        if(compare < 0){
          if(preemptCore == -1){
            preemptCore = i;
          }else{
            if(schedScheme == SJF){
              comparePreemptCore = comparerSJF(coreJobs[i], coreJobs[preemptCore]);
            }else{
              comparePreemptCore = comparerPRI(coreJobs[i], coreJobs[preemptCore]);
            }

            if(comparePreemptCore > 0){
              preemptCore = i;
            }

          }
          /*if(preemptCore == -1 || comparePreemptCore > 0){
            preemptCore = i;
          }*/
        }
      }
    }

    if(preemptCore != -1){
      job_t *preemptedJob = coreJobs[preemptCore];
      priqueue_offer(&jobQueue, preemptedJob);
      preemptedJob->coreID = -1;

      coreJobs[preemptCore] = newJob;
      newJob->coreID = preemptCore;
      newJob->startTime = time;

      priqueue_remove(&jobQueue, newJob);
      return preemptCore;
    }
  }

  
	return -1;
}



/**
  Called when a job has completed execution.
 
  The core_id, job_number and time parameters are provided for convenience. You may be able to calculate the values with your own data structure.
  If any job should be scheduled to run on the core free'd up by the
  finished job, return the job_number of the job that should be scheduled to
  run on core core_id.
 
  @param core_id the zero-based index of the core where the job was located.
  @param job_number a globally unique identification number of the job.
  @param time the current time of the simulator.
  @return job_number of the job that should be scheduled to run on core core_id
  @return -1 if core should remain idle.
 */
int scheduler_job_finished(int core_id, int job_number, int time)
{
  if(core_id >= coresNum || core_id < 0 || coreJobs[core_id] == NULL){
    return - 1;
  }

  job_t *finishedJob = coreJobs[core_id];
  finishedJob->completionTime = time;
  coreJobs[core_id] = NULL;
  if(finishedJob->jobID == job_number){
    priqueue_remove(&jobQueue, finishedJob);
  }
  


  if(priqueue_size(&jobQueue) > 0){
    job_t *nextScheduled = priqueue_poll(&jobQueue); //XX comeback check
    coreJobs[core_id] = nextScheduled;
    nextScheduled->coreID = core_id;
    if(nextScheduled->startTime == -1){
      nextScheduled->startTime = time;
    }

    return nextScheduled->jobID;
  }

	return -1;
}


/**
  When the scheme is set to RR, called when the quantum timer has expired
  on a core.
 
  If any job should be scheduled to run on the core free'd up by
  the quantum expiration, return the job_number of the job that should be
  scheduled to run on core core_id.

  @param core_id the zero-based index of the core where the quantum has expired.
  @param time the current time of the simulator. 
  @return job_number of the job that should be scheduled on core cord_id
  @return -1 if core should remain idle
 */
int scheduler_quantum_expired(int core_id, int time)
{
  if(schedScheme != RR || core_id >= coresNum || core_id < 0 || coreJobs[core_id] == NULL){
    return -1;
  }else{
    job_t *currentJob = coreJobs[core_id];

    if(priqueue_size(&jobQueue) == 0){
      return currentJob->jobID;
    }

    priqueue_offer(&jobQueue, currentJob);

    currentJob->coreID = -1;

    job_t *nextScheduled = priqueue_poll(&jobQueue);
    coreJobs[core_id] = nextScheduled;
    nextScheduled->coreID = core_id;
    if(nextScheduled->startTime == -1){
      nextScheduled->startTime = time;
    }

    return nextScheduled->jobID;
  }

	return -1;
}


/**
  Returns the average waiting time of all jobs scheduled by your scheduler.

  Assumptions:
    - This function will only be called after all scheduling is complete (all jobs that have arrived will have finished and no new jobs will arrive).
  @return the average waiting time of all jobs scheduled.
 */
float scheduler_average_waiting_time()
{
  if(totalJobs == 0){
    return 0.0;
  }

  float totalWaitTime = 0.0;
  for(int i = 0; i < totalJobs; i++){
    job_t *j = trackJobs[i];
    if(j->completionTime != -1){
      int waitTime = j->completionTime - j->arrivalTime - j->burstTime;
      totalWaitTime += waitTime;
    }
  }

  return totalWaitTime / totalJobs;
	//return 0.0;
}


/**
  Returns the average turnaround time of all jobs scheduled by your scheduler.

  Assumptions:
    - This function will only be called after all scheduling is complete (all jobs that have arrived will have finished and no new jobs will arrive).
  @return the average turnaround time of all jobs scheduled.
 */
float scheduler_average_turnaround_time()
{
  if(totalJobs == 0){
    return 0.0;
  }

  float totalTurnaround = 0.0;
  for(int i = 0; i < totalJobs; i++){
    job_t *j = trackJobs[i];
    if(j->completionTime != -1){
      int turnaroundTime = j->completionTime - j->arrivalTime;
      totalTurnaround += turnaroundTime;
    }
  }

  return totalTurnaround / totalJobs;
	//return 0.0;
}


/**
  Returns the average response time of all jobs scheduled by your scheduler.

  Assumptions:
    - This function will only be called after all scheduling is complete (all jobs that have arrived will have finished and no new jobs will arrive).
  @return the average response time of all jobs scheduled.
 */
float scheduler_average_response_time()
{
  if(totalJobs == 0){
    return 0.0;
  }
  
  float totalResponseTime = 0.0;
  for(int i = 0; i < totalJobs; i++){
    job_t *j = trackJobs[i];
    if(j->startTime != -1){
      int responseTime = j->startTime - j->arrivalTime;
      totalResponseTime += responseTime;
    }
  }

  return totalResponseTime / totalJobs;
	//return 0.0;
}


/**
  Free any memory associated with your scheduler.
 
  Assumption:
    - This function will be the last function called in your library.
*/
void scheduler_clean_up()
{
  for(int i = 0; i < totalJobs; i++){
    free(trackJobs[i]);
  }
  free(trackJobs);

  free(coreJobs);

  priqueue_destroy(&jobQueue);
}


/**
  This function may print out any debugging information you choose. This
  function will be called by the simulator after every call the simulator
  makes to your scheduler.
  In our provided output, we have implemented this function to list the jobs in the order they are to be scheduled. Furthermore, we have also listed the current state of the job (either running on a given core or idle). For example, if we have a non-preemptive algorithm and job(id=4) has began running, job(id=2) arrives with a higher priority, and job(id=1) arrives with a lower priority, the output in our sample output will be:

    2(-1) 4(0) 1(-1)  
  
  This function is not required and will not be graded. You may leave it
  blank if you do not find it useful.
 */
void scheduler_show_queue()
{
  printf("Current Queue Size: %d\n", priqueue_size(&jobQueue));
}
