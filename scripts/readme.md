An example running the replicate_database script using databases mounted in 
docker containers


First, run the source container:

```bash
sudo docker run --rm -d  --name pg-docker -p 5432:5432 postgres:10.6 

```

Then, run the destination container

```bash
sudo docker run --rm -d --name pg-docker_dest -p 5440:5432 postgres:10.6 
```

Finally, execute the replication script. The destination database will be assigned
with a name like copy_postgres_2019_03_01_17_15

```bash
.replicate_database.sh -sh 127.0.0.1 -dh 127.0.0.1 -dp 5440 -db postgres
```