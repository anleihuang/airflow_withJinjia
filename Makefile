export AIRFLOW_HOME := ${HOME}/airflow
export TMP_FILE := ${HOME}/airflow/dags/*.py
	
.PHONY : clean
clean:
	rm -rf output/*

.PHONY : install
install:
	mkdir example
	cd example
	python3 -m venv venv
	source ./venv/bin/activate
	pip install --upgrade pip
	pip install apache-airflow pyyaml jinja2 black 
	pip install google-api-python-client google-cloud-storage pandas-gbq

generate-dag:
	mkdir -p output
	python3 generate_dag.py
	black ./*.py

load-dag-test-seq:
	python output/example-sequential.py
	mkdir -p ${AIRFLOW_HOME}/dags
	if [ -e $(TMP_FILE) ]; then \
		echo "the py file exists. Remove the file";\
		rm $(TMP_FILE);\
	fi
	@echo "copy py file to ${AIRFLOW_HOME}/dags/"
	cp ./output/example-sequential.py ${AIRFLOW_HOME}/dags/example-sequential.py 
	airflow list_tasks example-sequential --tree
    
load-dag-test-par:
	python output/example-parallel.py
	mkdir -p ${AIRFLOW_HOME}/dags
	if [ -e $(TMP_FILE) ]; then \
		echo "the py file exists. Remove the file";\
		rm $(TMP_FILE);\
	fi
	@echo "copy py file to ${AIRFLOW_HOME}/dags/"
	cp ./output/example-parallel.py ${AIRFLOW_HOME}/dags/example-parallel.py 
	airflow list_tasks example-parallel --tree