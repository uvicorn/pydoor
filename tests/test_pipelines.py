from pydoor.pipelines import PipeLine as PL
from pydoor.base import Step
import pytest
from pydantic import ValidationError

@pytest.fixture()
def blank_pipeline():
    return PL()

@pytest.fixture()
def func_pipeline():
    def f(g:int):return g+1
    return PL(
        (lambda x:x+1),
        (lambda x:x+1),
        f,
    )

@pytest.fixture()
def get_steps():
    class Step1(Step): pass
    class Step2(Step1):
        def run(self, data: int):
            return data+1
    class Step3(Step2):pass
    class Step4(Step3):pass
    return (Step1, Step2, Step3, Step4) # +3


@pytest.fixture()
def steps_pipeline(get_steps):
    return PL(*get_steps)

def test_steps_pipe(steps_pipeline):
    assert steps_pipeline.run(0) == 3
    with pytest.raises(ValidationError) as er:
        steps_pipeline.run('ahahalol')

def test_func_pipe(func_pipeline):
    assert func_pipeline.run(0) == 3

