from typing import Any,
from pydantic import validate_arguments
from types import FunctionType
from .base import BasePipeLine, BaseStep, Step

def generate_pipes(steps: list)
    pass

class PipeLine(BasePipeLine):
    def run(self, data: Any) -> Any:
        le = len(self.steps)
        if not le: return
        pos = 0
        cache = data
        while pos < le:
            el = self.steps[pos]
            if isinstance(type(el), BaseStep):
                step = el.run(data=cache)
            elif isinstance(el, FunctionType):
                step = validate_arguments(el)(cache)
            elif issubclass(el, Step):
                step = el().run(data=cache)
            else:
                raise TypeError('Invalid type in pipeline')
            cache = step
            pos+=1
        return cache

class ThreadPipeLine(BasePipeLine):
    def run(self, data):
        pass 
