import typing as ty
from typing_extensions import dataclass_transform, Self
import attrs
import pydra
from pydra.engine.specs import LazyField


# P = ty.ParamSpec("P")
# RV = ty.TypeVar("RV")


# class FunctionTask:
#     func: ty.Callable
#     name: str
#     In: type
#     Out: type

#     def __init__(self, name: str, **kwargs):
#         self.name = name
#         self.inputs = type(self).In(**kwargs)

#     def __call__(self) -> "FunctionTask":
#         return type(self).func(
#             **{f.name: getattr(self.inputs, f.name) for f in attrs.fields(self.In)}
#         )


# def task(
#     maybe_function: ty.Optional[ty.Callable] = None,
#     returns: ty.Optional[ty.Iterable[str]] = None,
# ) -> ty.Callable[ty.Concatenate[str, P], RV]:
#     def decorator(
#         function: ty.Callable[P, RV]
#     ) -> ty.Callable[ty.Concatenate[str, P], RV]:
#         sig = inspect.signature(function)

#         inputs_dct = {
#             p.name: attrs.field(default=p.default) for p in sig.parameters.values()
#         }
#         inputs_dct["__annotations__"] = {
#             p.name: p.annotation for p in sig.parameters.values()
#         }

#         outputs_dct = {r: attrs.field() for r in (returns if returns else ["out"])}
#         if sig.return_annotation:
#             outputs_dct["__annotations__"] = {
#                 p.name: p.annotation for p in sig.return_annotation
#             }

#         @wraps(function, updated=())
#         @attrs.define(kw_only=True, slots=True, init=False)
#         class Task(FunctionTask):
#             func = function

#             In = attrs.define(type("In", (), inputs_dct))  # type: ty.Any
#             Out = attrs.define(type("Out", (), outputs_dct))  # type: ty.Any

#             inputs: In = attrs.field()

#         return Task

#     if maybe_function:
#         return decorator(maybe_function)
#     else:
#         return decorator

T = ty.TypeVar("T")


@attrs.define
class InBase(ty.Generic[T]):
    @classmethod
    def task_type(cls):
        base_class = cls.__orig_bases__[0]
        type_arg = base_class.__args__[0]
        if isinstance(type_arg, ty.ForwardRef):
            type_arg = type_arg._evaluate(globals(), locals(), frozenset())
        return type_arg

    def attach(self, workflow: pydra.Workflow, name=None) -> T:
        task_type = self.task_type()
        if name is None:
            name = task_type.__name__
        task = task_type(inputs=self, name=name)
        workflow.add(task)
        return task


@attrs.define
class OutBase(ty.Generic[T]):
    pass


@attrs.define(auto_attribs=False)
class BaseTask:
    @property
    def lzout(self):
        return LzOut(self)

    @classmethod
    def add_to(cls, wf: pydra.Workflow, inputs, name: ty.Optional[str] = None) -> Self:
        if name is None:
            name = cls.__name__
        task = cls(inputs=inputs, name=name)
        wf.add(task)
        return task

    name: str = attrs.field()
    inputs: InBase = attrs.field()
    lzout: OutBase  # type: ignore[no-redef]


class LzOut:
    def __init__(self, task: BaseTask):
        self.task = task

    def __getattr__(self, name):
        # tp = getattr(attrs.fields(self.task.In), name).type
        lf = LazyField(self.task, "output")
        lf.field = name
        return lf


class FunctionTask(BaseTask):
    function: ty.Callable

    def __call__(self):
        return self.function(**attrs.asdict(self.inputs))


@dataclass_transform()
def func_task(klass):
    return attrs.define(auto_attribs=False, kw_only=True)(klass)


@func_task
class MyTask(FunctionTask):
    @staticmethod
    def function(x: int, y: float) -> float:
        return x * y

    @attrs.define
    class In(InBase["MyTask"]):
        x: int = attrs.field()
        y: float = attrs.field()

    @attrs.define
    class Out(OutBase["MyTask"]):
        out: float = attrs.field()

    inputs: In
    lzout: Out


wf = pydra.Workflow(name="my_wf", input_spec=["x"])

task1 = MyTask.In(x=wf.lzin.x, y="bad").attach(wf)

task2 = MyTask.In(x=task1.lzout.out, y=task1.lzout.out).attach(wf, name="task2")

a: int = task1.inputs.y
