import pytest

from mistpy.decorators import *
from mistpy.executable_entry import is_mist_function, get_metadata

def test_no_args():
    @on_spark_context
    def sc1(sc):
        return sc

    meta = get_metadata(sc1)
    assert meta.args_info == set() 
    
