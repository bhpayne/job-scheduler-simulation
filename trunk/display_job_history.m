% Octave
clear
joblog=load('jobs.log');
[num_time_steps num_entries]=size(joblog);
num_nodes=(num_entries-1)/4;

node_id_ary=joblog(1,[2:4:end]);
time_ary=zeros(1,num_time_steps);
cpu_usage_ary=zeros(num_time_steps,num_nodes);
pwr_usage_ary=zeros(num_time_steps,num_nodes);
jobid_ary=    zeros(num_time_steps,num_nodes);
for time_indx = 1:num_time_steps
  time_ary(1,time_indx)=joblog(time_indx,1);
  cpu_usage_ary(time_indx,:)=joblog(time_indx,[3:4:end]);
  pwr_usage_ary(time_indx,:)=joblog(time_indx,[4:4:end]);
  jobid_ary(time_indx,:)=    joblog(time_indx,[5:4:end]);
endfor

close all; 
figure; pcolor(node_id_ary,time_ary,cpu_usage_ary);shading flat;drawnow;colorbar();
xlabel('node id');ylabel('time index');title('cpu use');

figure; pcolor(node_id_ary,time_ary,jobid_ary);shading flat;drawnow;colorbar();
xlabel('node id');ylabel('time index');title('job id');

