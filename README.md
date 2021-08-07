# DEV_apache_airflow_baseline

Project is created to setup baseline environment for quick DAGs implementation and their spin up.

## **About**

In my current example i'm going to implement several dags performing extraction of organizations contact details listed in [Direct118](http://www.118.direct) resource.

118 Information are the key providers of business listings information across the key search engines, online directories, sat navs, maps, POI and local listings publishers. 118 Information contacts over 2 million UK businesses every year to make sure their details are correct for listings used by BT, 118118, Bing.com and many others.

## **Tech stack**

* Python3
* Apache airflow ([documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html))
* PostgreSQL
* Docker & Docker-compose
* Beautiful Soup
* cloudscrapper

## **Start sequence**

1. Add `.env` file under path `env/.env.dev` and specify environment variables:
    * AIRFLOW_IMAGE_NAME: latest for now: `atmosphere4u/apache-airflow:2.1.2.1-python3.9`
    * _AIRFLOW_WWW_USER_USERNAME: Admin username 
    * _AIRFLOW_WWW_USER_PASSWORD: Admin password
    * AIRFLOW_UID: User id running airflow
    * AIRFLOW_GID: Don't know
    * AIRFLOW__CORE__LOAD_EXAMPLES=False # Ignore examples loading

2. `docker-compose --env-file ./airflow_env/startup_configs up airflow-init` : To start postgres and redis.
3. `docker-compose --env-file ./airflow_env/startup_configs up -d` : To start postgres and redis.

FAQ:
1. How to delete initlized data: `docker volume rm $(docker volume ls -q)`