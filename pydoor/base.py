from typing import Callable, Union, Type, Any
from pydantic import validate_arguments

class BaseStep(type):
    def __new__(cls, clsname, bases, dct):
        if 'run' in dct:
            fu = dct['run']
        elif 'run' in bases[0].__dict__:
            fu = bases[0].run
        else:
            for obj in bases[0].__mro__:
                if hasattr(obj, 'run'):
                    fu = obj.run
                    break

        dct['run'] = validate_arguments(fu)
        return super(BaseStep, cls).__new__(cls, clsname, bases, dct)


class Step(metaclass=BaseStep):
    def __init__(self, **kwa):
        for k ,v in kwa.items():
            setattr(self, k, v)

    def run(self, data: Any) -> Any:
        return data


class BasePipeLine(metaclass=BaseStep):
    def __init__(
        self,
        *steps: Union[Callable, 'BasePipeLine', Step, Type[Step]],
        **kwa
    ):
        self.steps = steps
        for k ,v in kwa.items():
            setattr(self, k, v)

    def run(self, data: Any) -> Any:
        return data


'''
# test
class Step1(Step):
    def run(self, data: str): return data

class Step3(Step1): pass

class Step4(Step): pass
class Step5(Step): pass
pipeline = PipeLine(
    Step1,
    (lambda x:x+1),
    Step3(r=123, t=123),
    PipeLine(Step4, Step5)
)
print(pipeline.run(123)) '''
