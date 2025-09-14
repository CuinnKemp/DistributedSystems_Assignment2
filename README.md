# Distributed Systems Assignment 2
The goal of this project is to implement 
an Aggregation Server, Get Client and Content Server.
The Aggregation Server Aggregates data sent via PUT
requests from Content Servers. This Station data is then stored.
A Get Client is then able to send a Get Request to the Aggregation 
Server to fetch the latest available data for all or a specific station
based on a lamport clock.

## Build: MakeFile
1. Build project:
``
make clean && make all
``
2. Run Aggregation Server:
``
make run-aggregation ARGS="<port>"
``
3. Run Content Server:
``
make run-content ARGS="<host>:<port> <path-to-data>"
``
4. Run Get Client:
``
make run-client ARGS="<host>:<port> [optional:stationId]"
``

## Test Maven
