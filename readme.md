
# Apache Airflow Capstone Project

The Apache Airflow Capstone Project is a culmination of the skills and knowledge gained through a comprehensive course on Apache Airflow. In this project, I have created a sophisticated workflow system using Apache Airflow to automate and orchestrate data processes.

## Environment Variables

To run this project, you will need to add the following environment variables to your Airflow

`FILE_PATH`: `/tmp`,
`openweathermapApi`: `{YOUR_API}`,
`slack_token`: `{xoxb-*}`
## Deployment
First of all read official documentation: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

Now that we have our docker-compose.yaml file, we can start it up!

This file contains several service definitions:

`airflow-scheduler` - The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.

`airflow-webserver` - The webserver is available at http://localhost:8080.

`airflow-worker` - The worker that executes the tasks given by the scheduler.

`airflow-triggerer` - The triggerer runs an event loop for deferrable tasks.

`airflow-init` - The initialization service.

`postgres` - The database.

`redis` - The redis - broker that forwards messages from scheduler to worker.
`vault` - The encrypted database for sensitive information

Optionally, you can enable flower by adding --profile flower option, e.g. docker compose --profile flower up, or by explicitly specifying it on the command line e.g. docker compose up flower.

`flower` - The flower app for monitoring the environment. It is available at http://localhost:5555.

Some directories in the container are mounted, which means that their contents are synchronized between your computer and the container.

`./dags` - you can put your DAG files here.

`./logs` - contains logs from task execution and scheduler.

`./plugins` - you can put your custom plugins here.

`./resources` - you can put your local datasets here

Make sure no other copies of the app/db are running first (`docker ps` and `docker rm -f <ids>`).

Start up the application stack using the docker compose up command. We’ll add the -d flag to run everything in the background.

```bash
  docker compose up -d
```

When we run this, we should see output like this:


 ```
[+] Running 8/8
 ✔ Container airflow_home-vault-1              Started                                                                          
 ✔ Container airflow_home-postgres-1           Healthy                                                                          
 ✔ Container airflow_home-redis-1              Healthy                                                                          
 ✔ Container airflow_home-airflow-init-1       Exited                                                                           
 ✔ Container airflow_home-airflow-triggerer-1  Started                                                                          
 ✔ Container airflow_home-airflow-webserver-1  Started                                                                          
 ✔ Container airflow_home-airflow-worker-1     Started 
 ✔ Container airflow_home-airflow-scheduler-1  Started  
 ```

**Note:** run `docker compose up -d` from `airflow_home` directory and use `docker-compose.yaml` from this project
## Authors

- [@mcoslet](https://github.com/mcoslet)

