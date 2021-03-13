from typing import Any, Callable, Type, Union
from pydantic import validate_arguments
from types import FunctionType
from .base import BasePipeLine, BaseStep, Step

def generate_pipes(steps: list, data):
    le = len(steps)
    if not le: return
    cache = data
    for el in steps:
        if isinstance(type(el), BaseStep):
            step = el.run(data=cache)
        elif isinstance(el, FunctionType):
            step = validate_arguments(el)(cache)
        elif issubclass(el, Step):
            step = el().run(data=cache)
        else:
            raise TypeError('Invalid type in pipeline')
        yield step

def get_step(el: Union[Type[Step], BasePipeLine, Step], cache):
    if isinstance(type(el), BaseStep):
        return el.run(data=cache)
    elif isinstance(el, FunctionType):
        return validate_arguments(el)(cache)
    elif issubclass(el, Step):
        return el().run(data=cache)
    else:
        raise TypeError('Invalid type in pipeline')

class PipeLine(BasePipeLine):
    def run(self, data: Any) -> Any:
        le = len(self.steps)
        if not le: return
        cache = data
        for el in self.steps:
            step = get_step(el, cache)
            cache = step
        return cache

class ThreadPipeLine(BasePipeLine):
    def run(self, data):
        pass # TODO
class AsyncPipeLine(BasePipeLine):
    def run(self, data, loop=None):
        pass # TODO

