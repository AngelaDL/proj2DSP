This repository contains a Data Stream Processing application using Apache Storm.

## Requirements ##
* Apache Storm 1.1.0
* Apache Kafka

### Compile your code ###
This project is configured as a Maven project. All you have to do to compile the code is running the following command from the root folder of the project (where the pom.xml file is located): 

```
mvn clean install package
```

### Run the application ###

First move to /dist directory and launch

```
sh startEnvironment.sh
```

Then, from /script directory launch

```
sh createTopic.sh
```

and

```
sh startConsumer.sh
```

Attach to the Storm CLI

```
docker attach storm-cli
```

and launch the topology you want to run

```
cd /data
sh launchTopology1.sh
```
or

```
cd /data
sh launchTopology2.sh
```

Finally, from /dist directory launch the Datasource
```
sh launchDatasource.sh
```