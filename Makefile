mytag=python3ubuntu23

docker_build:
	docker build --tag $(mytag) .

docker_run:
	docker run -it --rm -v`pwd`:/scratch -w/scratch $(mytag) /bin/bash

docker_sim:
	docker run -it --rm -v `pwd`:/scratch python3ubuntu23 python3 /scratch/multiple_HPC_systems.py

viz_Makefile:
	makefile2dot | dot -Tpng > Makefile_viz.png
