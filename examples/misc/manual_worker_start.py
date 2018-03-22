#!/usr/bin/python

import os
import subprocess

def fromEnv():
    return StartData( 
      os.getenv('MIST_MASTER_ADDRESS'),
      os.getenv('MIST_WORKER_NAME'),
      os.getenv('MIST_WORKER_RUN_OPTIONS'),
      os.getenv('MIST_WORKER_SPARK_CONF')
    )

class StartData:

    def __init__(self, master_addr, name, run_opts, spark_conf):
        self.master_addr = master_addr
        self.name = name
        self.run_opts = run_opts
        self.spark_conf = spark_conf
        print(self.run_opts)

    def __opts_list(self):
        parts = self.run_opts.split(' ')
        result = []
        for x in parts:
            stripped = x.strip()
            if (stripped != ''):
                result.append(stripped)
        return result

    def __confs_list(self):
        parts = self.spark_conf.split('|+|')
        result = []
        for x in parts:
            result.append('--conf')
            result.append(x)
        return result

    def to_spark_submit(self, spark_home, mist_home):
        submit = [os.path.join(spark_home, 'bin', 'spark-submit')]
        args = ['--class', 'io.hydrosphere.mist.worker.Worker', os.path.join(mist_home, 'mist-worker.jar'), '--master', self.master_addr, '--name', self.name]
        return submit + self.__opts_list() + self.__confs_list() + args

spark_home = os.getenv("SPARK_HOME")
mist_home = os.getenv("MIST_HOME")
cmd = fromEnv().to_spark_submit(spark_home, mist_home)
print("MANUAL STARTER: try start:" + ' '.join(cmd))
subprocess.call(cmd)
