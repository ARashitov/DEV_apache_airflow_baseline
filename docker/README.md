# `/Docker`

To keep everything working do changes only in the next:
* `Dockerfile`: Edit only
* `/airflow`: CRUD
* `install_private_dependencies.sh`: Edit only (*NOTE*: Don't commit your credentials to GITHUB)


## **How to build custom docker image for Airflow**

*Latest image available now*: `atmosphere4u/airflow:2.1.2.1`

1. export environment variable `AIRFLOW_IMAGE_NAME` (example: `export AIRFLOW_IMAGE_NAME=atmosphere4u/airflow:2.1.2.1`)
2. change directory using command `cd docker`
3. run command: `source build_airflow_image.sh`
4. *(optional)*: run command `source push_build_image_to_dockerhub.sh`
5. You are done! Feel free to edit `Dockerfile`