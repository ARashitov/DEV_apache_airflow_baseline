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


## Commands to manage local enironment

**Local environment containers**:
* Redis + Postgres (metadata airflow storage)
* Webserver + Scheduler + Flower + Worker(1)
* Postgres (for actual data storage)

```bash
docker-compose -f docker-compose-local.yaml --env-file ./environment/initials up airflow-init \
    && docker-compose -f docker-compose-local.yaml --env-file ./environment/initials up -d;
```

## Commands to manage local environment

File: `docker-compose-local.yaml`

**Local environment containers**:
* Redis + Postgres (metadata airflow storage)
* Webserver + Scheduler + Flower + Worker(1)
* Postgres (for actual data storage)

To start:
```bash
docker-compose -f docker-compose-local.yaml --env-file ./environment/initials up airflow-init \
    && docker-compose -f docker-compose-local.yaml --env-file ./environment/initials up -d;
```

To stop:
```bash
docker-compose -f docker-compose-local.yaml --env-file ./environment/initials down
```

## **FAQ**:
1. How to delete initlized data: `docker volume rm $(docker volume ls -q)`
