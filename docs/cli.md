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


```
./mist start cli

Hello! This is Mist command-line interface. Enter your command please.
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

list workers
Hello! This is Mist command-line interface.
address: akka.tcp://mist@127.0.0.1:41502 Name: streaming1
address: akka.tcp://mist@127.0.0.1:40104 Name: streaming3
address: akka.tcp://mist@127.0.0.1:41760 Name: streaming2
it's all workers

```


list all running jobs


```
./mist start cli --list jobs

Hello! This is Mist command-line interface.
list jobs
namespace: streaming2 extId: job2
it's all job descriptions from akka.tcp://mist@127.0.0.1:41760
namespace: streaming1 extId: job1
it's all job descriptions from akka.tcp://mist@127.0.0.1:41502
namespace: streaming3 extId: job3
it's all job descriptions from akka.tcp://mist@127.0.0.1:40104
exit
```


kill worker “streaming1”


```
./mist start cli --kill worker streaming1

Hello! This is Mist command-line interface.
kill worker streaming1
address: akka.tcp://mist@127.0.0.1:39547 Name: streaming3
address: akka.tcp://mist@127.0.0.1:39914 Name: streaming2
it's all workers
exit
```


kill job “job2”


```
./mist start cli --kill job job3

Hello! This is Mist command-line interface.
kill job job3
do`t worry, sometime job job3 will be stopped
namespace: streaming2 extId: job2
it's all job descriptions from akka.tcp://mist@127.0.0.1:39914
namespace: streaming3 extId: job3
it's all job descriptions from akka.tcp://mist@127.0.0.1:39547
exit

./mist start cli --list jobs         
Hello! This is Mist command-line interface.
list jobs 
namespace: streaming2 extId: job2
it's all job descriptions from akka.tcp://mist@127.0.0.1:39547
it's all job descriptions from akka.tcp://mist@127.0.0.1:39914
exit

```


