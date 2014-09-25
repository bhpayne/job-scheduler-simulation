#!/usr/bin/env python
# Ben Payne
# last updated 20140924
# created 20140924

import random
import math

number_of_timesteps=100
number_of_nodes=5

def random_power():
  rand_pwr=math.floor(random.random()*1000.0)/1000.0
  return rand_pwr

def random_cpu():
  rand_cpu=random_power()
  return rand_cpu

jobid=10
for time_indx in range(number_of_timesteps):
  node_report=""
  for node in range(number_of_nodes):
    node_report=node_report+" "+str(node)+" "+str(random_cpu())+" "+str(random_power())+" "+str(jobid)
    if (random.random()<0.1):
      jobid=jobid+1 
  print(str(time_indx)+node_report)

