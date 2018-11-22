# cloudtrax

script for consume cloudtrax events from kafka, normalize and inject into another kafka topic

# BUILDING

- Build docker image:
  * git clone https://github.com/wjjpt/cloudtrax-connect.git
  * cd src/
  * docker build -t wjjpt/cloudtrax .

# EXECUTING

- Execute app using docker image:

`docker run --env KAFKA_BROKER=X.X.X.X --env KAFKA_PORT=9092 -ti wjjpt/cloudtrax`

