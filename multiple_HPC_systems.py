#!/usr/bin/env python
# Ben Payne
# last updated 2013-05-10
# created 2013-05-09

# summary: run multiple concurrent simulations of HPC systems. If there are multiple HPC systems in a data center, then the power profile of the data center depends on usage profile of each HPC system.
# this simulation depends on "job_scheduler_model_scalar_parameters.py" which takes as an input "parameters.input"
# Thus, the inputs here necessitate a "parameters.input" for each HPC system in the data center. These inputs should obey the naming convention 
# parameters_0.input, parameters_1.input, parameters_2.input, etc. for each of the N-1 HPC systems


import os # calls to the host Operating System
import yaml # for reading parameters from file # http://pyyaml.org/wiki/PyYAML
import pickle # serialize data output
# import cPickle as pickle # "upto 1000 times faster because it is in C"

def read_parameters_input(file_name):
  with open(file_name,'r') as file_handle:
    input_data=yaml.load(file_handle)

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
 
  return number_of_jobs_to_run, total_number_of_nodes

number_of_HPC_systems=3

HPC_config=[]
for HPC_indx in range(number_of_HPC_systems):
  this_HPC_config=[]
  os.system('rm parameters.input')
  os.system('mv parameters_'+str(HPC_indx)+'.input parameters.input')
  os.system('python job_scheduler_model_scalar_parameters.py') # run the job


  os.system('mv schedule_nodes__power_used_time_resolved.pkl schedule_nodes__power_used_time_resolved_'+str(HPC_indx)+'.pkl')
  [number_of_jobs_to_run, total_number_of_nodes] = read_parameters_input('parameters.input')  
  this_HPC_config.append(number_of_jobs_to_run)
  this_HPC_config.append(total_number_of_nodes)
  
accumulated_HPC_results_nodes=[]
accumulated_HPC_results_power=[]
accumulated_HPC_results_job_concurrency=[]

for HPC_indx in range(number_of_HPC_systems):
  # format of file being read is 
  # time_step | #_of_nodes_running | power_used | number_of_jobs_running
  pkl_file=open('schedule_nodes__power_used_time_resolved_'+str(HPC_indx)+'.pkl','rb') # read
  number_of_nodes_running_time_resolved=pickle.load(pkl_file)
  power_used_time_resolved=pickle.load(pkl_file)
  number_of_jobs_running_time_resolved=pickle.load(pkl_file)
  pkl_file.close()

  # accumulate (normalized merge) the time-resolved statistics for node usage, power usage
  accumulated_HPC_results_nodes.append(number_of_nodes_running_time_resolved)
  accumulated_HPC_results_power.append(power_used_time_resolved)
  accumulated_HPC_results_job_concurrency.append(number_of_jobs_running_time_resolved)
  
