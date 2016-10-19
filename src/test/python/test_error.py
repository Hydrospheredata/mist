from mist.mist_job import *

class TestError(MistJob):

    def do_stuff(self, parameters):
        result = 1/0
        return result
