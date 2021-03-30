# fligooChallenge

1. Download the project in a folder called airflow_13
2. Position yourself in the airflow_13 folder
3. Run echo -e "AIRFLOW_UID = $ (id -u) \ nAIRFLOW_GID = 0"> .env 
4. Run sudo docker-compose up airflow-init
5. Run sudo docker-compose up --build
6. run sudo docker exec -it airflow_13_airflow-webserver_1 airflow connections add 'postgres' --conn-host 'airflow_13_postgres_testfligoo_1' --conn-login 'testfligoo' --conn-password 'testfligoo' --conn-port '5432' --conn-schema 'testfligoo' --conn-type 'Postgres' 
7. View ETL Proccess http://localhost:8080/
8. View Data: Jupiter Notebook "Read Flights.ipynb"
