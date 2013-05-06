#!/usr/bin/env python
# Ben Payne
# last updated 20130501
# created 20130426
# job scheduling model
# accounts for job time, node count, and power usage

# Description:
# There are two main arrays, "job_pool" and "jobs_running"
# Initially job_pool is populated with a list of jobs. Each job has four parameters: unique job ID, number of nodes, time to run, and power per node.
# The distributions for each of these parameters are determined by the user. The user also sets how many total jobs to run through the cluster, total number of nodes in the cluster, and a power cap.
# Time is then incremented from 0. At each time step, the scheduler determines whether there are nodes (and possibly power) available. If there is, move a job from the job_pool to jobs_running.
# At each time step, the jobs in jobs_running have their time decremented by 1. If the time reaches zero, the job is removed from jobs_running
# The output of the simulation is the number of nodes in use and total power in use at each time step. From these records the histogram of node and power usage can be created.
# The schedulers used are not "fair" and do not account for node locality. 

# Assumptions: 
# -homogeneous cluster = all nodes are interchangeable for run time and power usage. (Unrealistic.)
# -locality doesn't matter. (Unrealistic.)
# -topology is irrelevant to job run time. (Unrealistic.)
# -no node fails. Either the entire cluster is available or not. (Restarting the cluster can be modeled.) (Unrealistic.)
# -job power is uniform across all nodes. (Unrealistic.)
# -power per node is normalized to 1
# -time is normalized to 1
# -there are no "background" jobs available -- a job which will consume otherwise idle nodes when none of the jobs in the pool queue fit free nodes. See note in record_node_and_power_use()

# caveat: there are transient effects associated with t=0 (since all nodes are empty at that time). 
# To avoid transients, either ignore the statistics associated with small t (small compared to a*(mean job time)) or run for a long time (aka many jobs)
# The transients associated with t=0 may be realistic in that when a cluster is rebooted, all jobs die and must be restarted. If this effect is to be included, it makes sense to make many runs (thus including multiple t=0) and have mtbf as the reset time.
# same caveat applies for when the pool of job approaches being empty or the fixed number of jobs reaches termination: the statistics are not realistic.

# caveat: it's easy to get unrealistic results by altering the input distribution features. 
# Therefore realistic input distributions are vital to making useful predictions.

# package dependencies
import random # used for distributions
#import math 
import matplotlib.pyplot as plt # for generating plots # http://matplotlib.org/
import yaml # for reading parameters from file # http://pyyaml.org/wiki/PyYAML

#*****************************
def add_jobs_to_pool(number_of_jobs_to_add_to_pool,nodes_per_job_mean,nodes_per_job_stddev,total_number_of_nodes,wall_time_mean,wall_time_stddev,max_job_time,power_usage_mean,power_usage_stddev,power_usage_minimum,job_pool,start_job_ID):
  for job_indx in range(start_job_ID,start_job_ID+number_of_jobs_to_add_to_pool):
    this_job=[]
    #
    this_job.append(job_indx)
    # node count
    nodes_for_this_job=int(random.gauss(nodes_per_job_mean,nodes_per_job_stddev))
    if (nodes_for_this_job>total_number_of_nodes):
      nodes_for_this_job=total_number_of_nodes
    if (nodes_for_this_job<1):
      nodes_for_this_job=1
    this_job.append(nodes_for_this_job)
    #
    # time
    this_job_wall_time=int(random.gauss(wall_time_mean,wall_time_stddev))
    if (this_job_wall_time>max_job_time):
      this_job_wall_time=max_job_time
    if (this_job_wall_time<1):
      this_job_wall_time=1
    this_job.append(this_job_wall_time)
    #
    # power
    this_job_power_usage=random.gauss(power_usage_mean,power_usage_stddev)
    # http://docs.python.org/2/library/random.html#random.gauss
    if (this_job_power_usage>1):
      this_job_power_usage=1
    if (this_job_power_usage<power_usage_minimum):
      this_job_power_usage=power_usage_minimum
    this_job.append(this_job_power_usage)
    #
    job_pool.append(this_job)  

#*****************************
# "nodes available" scheduling simulation. Process a finite set of jobs from the job pool
# not "fair" and not "FIFO"
def scheduler_nodes_available(number_of_jobs_to_add_to_pool,number_of_jobs_to_run,total_number_of_nodes,power_cap):
  nodes_in_use=0 # initially the cluster is empty
  number_of_jobs_completed=0
  job_ID_increment=number_of_jobs_to_add_to_pool

  node_tracking=[]
  power_tracking=[]
  concurrency_tracking=[]

  jobs_running=[]
  jobs_which_ran=[]

  time_step=0
  print("\ntime="+str(time_step))
  while(number_of_jobs_completed<=number_of_jobs_to_run): 
    # how many nodes are available at this time?
    nodes_available=total_number_of_nodes-nodes_in_use
    if (nodes_available>total_number_of_nodes):
      print("ERROR with node count: "+str(nodes_available))
      exit()
    print("nodes available: "+str(nodes_available)+"; nodes in use: "+str(nodes_in_use))

    # given that many nodes at this time, add jobs from the pool
    number_of_available_jobs=len(job_pool)
    for job_indx in range(number_of_available_jobs): # find jobs in the pool to fit into the cluster
      job_id              =job_pool[job_indx][0]
      nodes_for_this_job  =job_pool[job_indx][1]
     #this_job_wall_time  =job_pool[job_indx][2]
     #this_job_power_usage=job_pool[job_indx][3]
      job_is_running=False
      if (nodes_for_this_job<=nodes_available and not job_is_running ): # then run the job
        nodes_in_use=nodes_in_use+nodes_for_this_job
        nodes_available=total_number_of_nodes-nodes_in_use
        jobs_running.append(job_pool[job_indx])
        this_job_and_when_it_started=job_pool[job_indx].append(time_step)
        jobs_which_ran.append(this_job_and_when_it_started)
#     print("after adding jobs,")
#     for running_job_indx in range(len(jobs_running)):
#       print("running job: "+str(jobs_running[running_job_indx]))  
#     print("nodes remaining: "+str(nodes_available))
    # now that we have a set of running jobs, remove those from the list of jobs to be run    
    for running_job_indx in range(len(jobs_running)):
      print("  running job = "+str(jobs_running[running_job_indx]))
      try: job_pool.remove(jobs_running[running_job_indx])
      except ValueError: pass # this job had already been removed from the job pool

#     print("after pruning job pool,")  
#     for pool_job_indx in range(len(job_pool)):
#       print("job in pool: "+str(job_pool[pool_job_indx]))
  
    # for each of the running jobs, decrement the time by 1
    [jobs_running,number_of_jobs_completed,job_ID_increment] = decrement_time_for_running_jobs(jobs_running,number_of_jobs_completed,number_of_jobs_to_run,nodes_per_job_mean,nodes_per_job_stddev,total_number_of_nodes,wall_time_mean,wall_time_stddev,max_job_time,power_usage_mean,power_usage_stddev,power_usage_minimum,job_pool,job_ID_increment)
    
#    print("after job finished")  
    [node_tracking,power_tracking,concurrency_tracking]=record_node_and_power_use(node_tracking,power_tracking,concurrency_tracking,jobs_running,total_number_of_nodes,power_usage_minimum)

#    for pool_job_indx in range(len(job_pool)):
#      print("  job in pool: "+str(job_pool[pool_job_indx]))

    # update the number of nodes in use
    nodes_in_use=0
    for running_job_indx in range(len(jobs_running)):
      nodes_in_use=nodes_in_use+jobs_running[running_job_indx][1]
    time_step=time_step+1  
    print("\ntime="+str(time_step))  
    
    print("running: "+str(len(jobs_running))+" in pool: "+str(len(job_pool))+" ran: "+str(number_of_jobs_completed))

  return node_tracking, power_tracking, concurrency_tracking, jobs_which_ran


#*****************************
# "nodes and power available" scheduling simulation. Process a finite set of jobs from the job pool
# not "fair" and not "FIFO"
def scheduler_nodes_and_power_available(number_of_jobs_to_add_to_pool,number_of_jobs_to_run,total_number_of_nodes,power_cap):
  nodes_in_use=0 # initially the cluster is empty
  power_in_use=0 # initially the cluster is in the off state
  total_power = total_number_of_nodes # since power per node is normalized to 1
  number_of_jobs_completed=0
  job_ID_increment=number_of_jobs_to_add_to_pool

  node_tracking=[]
  power_tracking=[]
  concurrency_tracking=[]

  jobs_running=[]
  jobs_which_ran=[]  

  time_step=0
  print("\ntime="+str(time_step))
  while(number_of_jobs_completed<=number_of_jobs_to_run): 
    # how much power is available at this time?
    power_available=total_power-power_in_use
    if (power_available>total_power):
      print("ERROR with power: "+str(power_available))
      exit()
    print("power available: "+str(power_available)+"; power in use: "+str(power_in_use))

    # AND how many nodes are available at this time?
    nodes_available=total_number_of_nodes-nodes_in_use
    if (nodes_available>total_number_of_nodes):
      print("ERROR with node count: "+str(nodes_available))
      exit()
    print("nodes available: "+str(nodes_available)+"; nodes in use: "+str(nodes_in_use))

    # given that many nodes and that much power at this time, add jobs from the pool
    number_of_available_jobs=len(job_pool)
    for job_indx in range(number_of_available_jobs): # find jobs in the pool to fit into the cluster
      job_id              =job_pool[job_indx][0]
      nodes_for_this_job  =job_pool[job_indx][1]
     #this_job_wall_time  =job_pool[job_indx][2]
      this_job_power_usage=job_pool[job_indx][3]*nodes_for_this_job
      job_is_running=False
      if ((nodes_for_this_job<=nodes_available) and (this_job_power_usage<=(power_available*power_cap)) and not job_is_running ): # then run the job
        nodes_in_use=nodes_in_use+nodes_for_this_job
        nodes_available=total_number_of_nodes-nodes_in_use
        power_in_use=power_in_use+this_job_power_usage
        power_available=total_power-power_in_use
        jobs_running.append(job_pool[job_indx])
        
        jobs_which_ran.append(job_pool[job_indx])

#     print("after adding jobs,")
#     for running_job_indx in range(len(jobs_running)):
#       print("running job: "+str(jobs_running[running_job_indx]))  

    # now that we have a set of running jobs, remove those from the list of jobs to be run    
    for running_job_indx in range(len(jobs_running)):
      print("  running job = "+str(jobs_running[running_job_indx]))
      try: job_pool.remove(jobs_running[running_job_indx])
      except ValueError: pass # this job had already been removed from the job pool

#     print("after pruning job pool,")  
#     for pool_job_indx in range(len(job_pool)):
#       print("job in pool: "+str(job_pool[pool_job_indx]))
  
    # for each of the running jobs, decrement the time by 1
    [jobs_running,number_of_jobs_completed,job_ID_increment] = decrement_time_for_running_jobs(jobs_running,
                                             number_of_jobs_completed,number_of_jobs_to_run,nodes_per_job_mean,
                                             nodes_per_job_stddev,total_number_of_nodes,wall_time_mean,
                                             wall_time_stddev,max_job_time,power_usage_mean,power_usage_stddev,
                                             power_usage_minimum,job_pool,job_ID_increment)

 #    print("after job finished")  
    [node_tracking,power_tracking,concurrency_tracking]=record_node_and_power_use(node_tracking,power_tracking,concurrency_tracking,jobs_running,total_number_of_nodes,power_usage_minimum)

#    for pool_job_indx in range(len(job_pool)):
#      print("  job in pool: "+str(job_pool[pool_job_indx]))

    # update the number of nodes in use
    nodes_in_use=0
    power_in_use=0
    for running_job_indx in range(len(jobs_running)):
      nodes_in_use=nodes_in_use+jobs_running[running_job_indx][1]
      power_in_use=power_in_use+jobs_running[running_job_indx][3]*(nodes_for_this_job/total_number_of_nodes)
    
    time_step=time_step+1  
    print("\ntime="+str(time_step))  
    
    print("running: "+str(len(jobs_running))+" in pool: "+str(len(job_pool))+" ran: "+str(number_of_jobs_completed))

  return node_tracking, power_tracking, concurrency_tracking, jobs_which_ran

#*****************************
# "power available" scheduling simulation. Process a finite set of jobs from the job pool
# not "fair" and not "FIFO"
def scheduler_power_available(number_of_jobs_to_add_to_pool,number_of_jobs_to_run,total_number_of_nodes,power_cap):
  power_in_use=0 # initially the cluster is in the off state
  total_power = total_number_of_nodes # since power per node is normalized to 1
  number_of_jobs_completed=0
  job_ID_increment=number_of_jobs_to_add_to_pool

  node_tracking=[]
  power_tracking=[]
  concurrency_tracking=[]

  jobs_running=[]
  jobs_which_ran=[]  

  time_step=0
  print("\ntime="+str(time_step))
  while(number_of_jobs_completed<=number_of_jobs_to_run): 
    # how much power is available at this time?
    power_available=total_power-power_in_use
    if (power_available>total_power):
      print("ERROR with power: "+str(power_available))
      exit()
    print("power available: "+str(power_available)+"; power in use: "+str(power_in_use))

    # given that much power at this time, add jobs from the pool
    number_of_available_jobs=len(job_pool)
    for job_indx in range(number_of_available_jobs): # find jobs in the pool to fit into the cluster
      job_id              =job_pool[job_indx][0]
      nodes_for_this_job  =job_pool[job_indx][1]
     #this_job_wall_time  =job_pool[job_indx][2]
      this_job_power_usage=job_pool[job_indx][3]*(nodes_for_this_job)
      job_is_running=False
      if ((this_job_power_usage<=(power_available*power_cap)) and not job_is_running ): # then run the job
        power_in_use=power_in_use+this_job_power_usage
#         print("job: "+str(job_pool[job_indx]))
#         print("power in use: "+str(power_in_use))
        power_available=total_power-power_in_use
#         print("power available: "+str(power_available))
        jobs_running.append(job_pool[job_indx])
        this_job_and_when_it_started=job_pool[job_indx].append(time_step)
        jobs_which_ran.append(this_job_and_when_it_started)

    # now that we have a set of running jobs, remove those from the list of jobs to be run    
    for running_job_indx in range(len(jobs_running)):
      print("  running job = "+str(jobs_running[running_job_indx]))
      try: job_pool.remove(jobs_running[running_job_indx])
      except ValueError: pass # this job had already been removed from the job pool
  
    # for each of the running jobs, decrement the time by 1
    [jobs_running,number_of_jobs_completed,job_ID_increment] = decrement_time_for_running_jobs(jobs_running,
                                             number_of_jobs_completed,number_of_jobs_to_run,nodes_per_job_mean,
                                             nodes_per_job_stddev,total_number_of_nodes,wall_time_mean,
                                             wall_time_stddev,max_job_time,power_usage_mean,power_usage_stddev,
                                             power_usage_minimum,job_pool,job_ID_increment)

 #    print("after job finished")  
    [node_tracking,power_tracking,concurrency_tracking]=record_node_and_power_use(node_tracking,power_tracking,concurrency_tracking,jobs_running,total_number_of_nodes,power_usage_minimum)

    # update the number of nodes in use
    power_in_use=0
    for running_job_indx in range(len(jobs_running)):
      power_in_use=power_in_use+jobs_running[running_job_indx][3]*(nodes_for_this_job/total_number_of_nodes)
    
    time_step=time_step+1  
    print("\ntime="+str(time_step))  
    
    print("running: "+str(len(jobs_running))+" in pool: "+str(len(job_pool))+" ran: "+str(number_of_jobs_completed))

  return node_tracking, power_tracking, concurrency_tracking, jobs_which_ran

#*****************************
# used by all schedulers
def decrement_time_for_running_jobs(jobs_running,number_of_jobs_completed,
                                    number_of_jobs_to_run,nodes_per_job_mean,nodes_per_job_stddev,
                                    total_number_of_nodes,wall_time_mean,wall_time_stddev,
                                    max_job_time,power_usage_mean,power_usage_stddev,power_usage_minimum,
                                    job_pool,job_ID_increment):
# print("looking for jobs that finished")
  jobs_continuing=[]
  for running_job_indx in range(len(jobs_running)):
    jobs_running[running_job_indx][2]=jobs_running[running_job_indx][2]-1
    if (jobs_running[running_job_indx][2]>0):
      jobs_continuing.append(jobs_running[running_job_indx])
    else:
      print("FINISHED job "+str(jobs_running[running_job_indx]))
      number_of_jobs_completed=number_of_jobs_completed+1
      job_ID_increment=job_ID_increment+1
      if (number_of_jobs_completed<number_of_jobs_to_run): # for each finished job, add another job to the pool
        add_jobs_to_pool(1,nodes_per_job_mean,nodes_per_job_stddev,total_number_of_nodes,wall_time_mean,wall_time_stddev,max_job_time,power_usage_mean,power_usage_stddev,power_usage_minimum,job_pool,job_ID_increment)
  jobs_running=[]
  jobs_running=jobs_continuing    
  return jobs_running,number_of_jobs_completed,job_ID_increment

#*****************************
# used by all schedulers
def record_node_and_power_use(node_tracking,power_tracking,concurrency_tracking,jobs_running,total_number_of_nodes,power_usage_minimum):
  node_tracking_at_this_time=0
  power_tracking_at_this_time=0
  concurrency_tracking.append(len(jobs_running))
  for running_job_indx in range(len(jobs_running)):
    node_tracking_at_this_time =node_tracking_at_this_time +jobs_running[running_job_indx][1]
    power_tracking_at_this_time=power_tracking_at_this_time+jobs_running[running_job_indx][3]*jobs_running[running_job_indx][1]
  # in addition to the power being used by nodes running jobs, the idle nodes also consume power. 
  nodes_idle = total_number_of_nodes-node_tracking_at_this_time # How many of the nodes are idle?
  # Note: if, instead of idle nodes, there should be a "background" type job which runs, then replace "power_usage_minimum" with the power used by the background job.
  power_idle = nodes_idle*power_usage_minimum # total power being spent on idle nodes
  power_tracking_at_this_time = power_tracking_at_this_time+power_idle # add the "idle powered nodes" to the total power

  nodes_used_percentage = (node_tracking_at_this_time/(total_number_of_nodes*1.0))*100
  if (nodes_used_percentage>100):
    print("ERROR in nodes used percentage: "+str(nodes_used_percentage))
    exit()
  node_tracking.append(nodes_used_percentage)

  power_used_percentage = (power_tracking_at_this_time/total_number_of_nodes)*100
  if (power_used_percentage>100):
    print("ERROR in power used percentage: "+str(nodes_used_percentage))
    exit()
  power_tracking.append(power_used_percentage)

  return node_tracking,power_tracking,concurrency_tracking

#*****************************
def save_results_to_file(node_tracking, power_tracking, concurrency_tracking, jobs_which_ran):
  f = open('schedule_nodes_power_used.dat','w')
  for lin in range(len(node_tracking)):
    f.write(str(lin)+"  "+str(node_tracking[lin])+"  "+str(lin)+"  "+str(power_tracking[lin])+"  "+str(lin)+"  "+str(concurrency_tracking[lin])+"\n")
  f.close()  
#   print("jobs which ran: \n"+str(jobs_which_ran))

#*****************************
def make_plots(node_tracking,power_tracking):
  plt.figure(1)
  plt.title('node utilization as function of time')
  plt.xlabel('time [AU]')
  plt.ylabel('nodes in use (%)')
  # plt.plot(range(len(node_tracking)),node_tracking,marker='o',markersize=4,linestyle='--')  
  plt.plot(range(len(node_tracking)),node_tracking,marker='o',markersize=4,linestyle='None')  
  plt.savefig("jobschedulersim_fig1_node_utilization_vs_time.png")
  plt.show()
  #plt.close()

  plt.figure(2)
  plt.title('node utilization histogram')
  plt.xlabel('nodes in use (%)')
  plt.ylabel('normalized count')
  plt.hist(node_tracking,bins=20, normed=True)
  #plt.hist(node_tracking, bins=20, normed=True, cumulative=True)
  plt.savefig("jobschedulersim_fig2_node_utilization_histogram.png")
  plt.show()
  #plt.close()

  plt.figure(3)
  plt.title('power utilization as function of time')
  plt.xlabel('time [AU]')
  plt.ylabel('power in use (%)')
  plt.plot(range(len(power_tracking)),power_tracking,marker='o',markersize=4,linestyle='None')  
  plt.savefig("jobschedulersim_fig3_power_utilization_vs_time.png")
  plt.show()
  #plt.close()

  plt.figure(4)
  plt.title('power utilization histogram')
  plt.xlabel('power in use (%)')
  plt.ylabel('normalized count')
  plt.hist(power_tracking,bins=20, normed=True)
  plt.savefig("jobschedulersim_fig4_power_utilization_histogram.png")
  plt.show()
  #plt.close()

  power_for_jobs=[]
  nodes_for_jobs=[]
  for job_indx in range(len(jobs_which_ran)):
    nodes_for_jobs.append(jobs_which_ran[job_indx][1])
    power_for_jobs.append(jobs_which_ran[job_indx][3])

  plt.figure(5)
  plt.title('power requests for all jobs that ran')
  plt.xlabel('power requests (%)')
  plt.ylabel('normalized count')
  plt.hist(power_for_jobs,bins=20, normed=True)
  plt.savefig("jobschedulersim_fig5_power_requests.png")
  plt.show()
  #plt.close()

  plt.figure(6)
  plt.title('node requests for all jobs that ran')
  plt.xlabel('node count requests (%)')
  plt.ylabel('normalized count')
  plt.hist(nodes_for_jobs,bins=20, normed=True)
  plt.savefig("jobschedulersim_fig6_node_requests.png")
  plt.show()
  #plt.close()


# done with function definitions
#*****************************
# parameter definitions

input_stream=file('parameters.input','r')
input_data=yaml.load(input_stream)

number_of_jobs_to_run=input_data["number_of_jobs_to_run"]
number_of_jobs_to_add_to_pool=input_data["number_of_jobs_to_add_to_pool"]
power_cap=input_data["power_cap"]
# node count [1]
total_number_of_nodes=input_data["total_number_of_nodes"]
nodes_per_job_mean=input_data["nodes_per_job_mean"]
nodes_per_job_stddev=input_data["nodes_per_job_stddev"]
# time [2]
max_job_time=input_data["max_job_time"]
wall_time_mean=input_data["wall_time_mean"]
wall_time_stddev=input_data["wall_time_stddev"]
# power [3]
power_usage_mean=input_data["power_usage_mean"]
power_usage_stddev=input_data["power_usage_stddev"]
power_usage_minimum=input_data["power_usage_minimum"]

input_stream.close() # done reading parameters input


# done with parameter setting
#*****************************
# main body of simulation

# create the pool of jobs which will be in the queue at t=0
start_job_ID=0
job_pool=[]
add_jobs_to_pool(number_of_jobs_to_add_to_pool,nodes_per_job_mean,nodes_per_job_stddev,total_number_of_nodes,wall_time_mean,wall_time_stddev,max_job_time,power_usage_mean,power_usage_stddev,power_usage_minimum,job_pool,start_job_ID)

# what's fascinating is that although the cluster runs at 100% node usage, the power usage rarely exceeds 90%. Thus this simple model captures the essential features!
#[node_tracking, power_tracking, concurrency_tracking, jobs_which_ran] = scheduler_nodes_available(number_of_jobs_to_add_to_pool,number_of_jobs_to_run,total_number_of_nodes,power_cap)
# or 
# as long as power per node for a given job is less than 1, "nodes and power" will be same as "nodes" alone.
[node_tracking, power_tracking, concurrency_tracking, jobs_which_ran] = scheduler_nodes_and_power_available(number_of_jobs_to_add_to_pool,number_of_jobs_to_run,total_number_of_nodes,power_cap)
# or 
# by power alone fails because nodes are oversubscribed.
#[node_tracking, power_tracking, concurrency_tracking, jobs_which_ran] = scheduler_power_available(number_of_jobs_to_add_to_pool,number_of_jobs_to_run,total_number_of_nodes,power_cap)

save_results_to_file(node_tracking, power_tracking, concurrency_tracking, jobs_which_ran)

# we care about the histogram of power usage, trimming the first and last parts to remove bias

# in Octave:
# datfil=load("schedule_nodes_power_used.dat");
# figure; plot(datfil(:,1),datfil(:,2));

make_plots(node_tracking,power_tracking)

