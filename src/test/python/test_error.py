from mist.mist_job import *

class TestError(MistJob):

    def execute(self):
        result = 1/0
        return {"result": result}
