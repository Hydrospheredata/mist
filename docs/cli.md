### Mist command-line interface


Mist CLI is a simple command-line interface to the Mist service.


This is where cli comes into play, cli makes mist job managing organized and easy for make out!


Usage:
```
./mist start cli

    --list workers              list all started workers
    --list jobs                 list all started jobs
    --kill all                  kill all workers
    --kill worker <namespace>   kill named worker
    --kill job <external id>    kill named job
```


You can easily run CLI and this will give mist command-line interface.


```./mist start cli```


Hello! This is Mist command-line interface. Enter your command please.


```
mist>_
mist>help (Enter)

    list workers               list all started workers
    list jobs                  list all started jobs
    kill all                   kill all workers
    kill worker <namespace>    kill named worker
    kill job    <external id>  kill named job
    exit
```


Also you can run command from shell, and make back in shell after running command

####Examples

list all running workers


```
./mist start cli --list workers

address: 1234 Name: streaming1
address: 2345 Name: streaming2
address: 3456 Name: streaming3

```


list all running jobs


```
./mist start cli --list jobs

 namespace: streaming1 extId: job1
 namespace: streaming2 extId: job2
 namespace: streaming3 extId: job3
```


kill worker “streaming1”


```./mist start cli --kill worker streaming1```


kill job “job2”


```./mist start cli --kill job job2```
