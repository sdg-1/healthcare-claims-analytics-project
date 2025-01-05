# Airflow Docker Compose Setup

This repository contains a Docker Compose configuration for setting up Apache Airflow with Postgres as the metadata database and an additional operations database.

## Features
- Apache Airflow (version 2.10.3) with a **LocalExecutor**
- Postgres metadata database for Airflow
- Separate operations Postgres database with custom initialization scripts
- Healthcheck for Airflow webserver
- Volume mounts for DAGs and logs

## Requirements
- Docker
- Docker Compose

## Setup Instructions
1. Start the Docker containers:
   ```bash
   docker-compose up -d
   ```

2. Access the Airflow webserver:
   Open a browser and navigate to [http://localhost:8080](http://localhost:8080). Use the following credentials to log in:
   - Username: `admin`
   - Password: `admin`

3. Add your DAGs:
   Place your DAG files in the `dags` directory (`/basic-airflow/dags`).

4. View logs:
   Logs are available in the `logs` directory (`/basic-airflow/logs`).

5. Other steps

You'll also need to download claims and beneficiary data from [here](https://data.cms.gov/collection/synthetic-medicare-enrollment-fee-for-service-claims-and-prescription-drug-event) to use for the files I have currently hard-coded.

- [Claims](https://data.cms.gov/sites/default/files/2023-04/67157de9-d962-4af0-bf0e-3578b3afec58/inpatient.csv)
- [Beneficiaries](https://data.cms.gov/sites/default/files/2023-04/d5da04d5-61c8-4174-be11-02d0f58217e7/beneficiary_2015.csv)

Currently there are also few config set-up issues that need to be improved. For example, snowflake_default will need to be created. If you haven't set up an Airflow connection, you should read [this](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html). I am sure there are a few other issues. I just wanted to get this out there in case people wanted to check out the code, but I'll be working on it as I have time. But also feel free to raise issues.

### Other Issues

- Need to add AIRFLOW_HOME in config and add it to the set-up steps above
  

## Customization
- Update environment variables in the `docker-compose.yml` file for your specific use case.
- Modify the `operations-db-init` directory to include SQL scripts for initializing the operations database.

## Stop Services
To stop the containers:
```bash
docker-compose down
```


## TODO
- Build S3 trigger via event bridge to fire when files are dropped into it
- Create a process to run the create data warehouse scripts on snowflake
- Build out transforms for other entities
- Add snowflake_default to the config set-up to pick up from a local file

## Troubleshooting
- Ensure Docker and Docker Compose are installed and running.
- Check logs using:
  ```bash
  docker-compose logs
  ```

