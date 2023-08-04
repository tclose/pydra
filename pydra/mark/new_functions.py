import typing as ty
import attrs
import inspect
from functools import wraps

P = ty.ParamSpec("P")
RV = ty.TypeVar("RV")


class FunctionTask:
    func: ty.Callable
    name: str
    Inputs: type
    Outputs: type

    def __init__(self, name: str, **kwargs):
        self.name = name
        self.inputs = type(self).Inputs(**kwargs)

    def __call__(self) -> "FunctionTask":
        return type(self).func(
            **{f.name: getattr(self.inputs, f.name) for f in attrs.fields(self.Inputs)}
        )


def task(
    maybe_function: ty.Optional[ty.Callable] = None,
    returns: ty.Optional[ty.Iterable[str]] = None,
) -> ty.Callable[ty.Concatenate[str, P], RV]:
    def decorator(
        function: ty.Callable[P, RV]
    ) -> ty.Callable[ty.Concatenate[str, P], RV]:
        sig = inspect.signature(function)

        inputs_dct = {
            p.name: attrs.field(default=p.default) for p in sig.parameters.values()
        }
        inputs_dct["__annotations__"] = {
            p.name: p.annotation for p in sig.parameters.values()
        }

        outputs_dct = {r: attrs.field() for r in (returns if returns else ["out"])}
        if sig.return_annotation:
            outputs_dct["__annotations__"] = {
                p.name: p.annotation for p in sig.return_annotation
            }

        @wraps(function, updated=())
        @attrs.define(kw_only=True, slots=True, init=False)
        class Task(FunctionTask):
            func = function

            Inputs = attrs.define(type("Inputs", (), inputs_dct))  # type: ty.Any
            Outputs = attrs.define(type("Outputs", (), outputs_dct))  # type: ty.Any

            inputs: Inputs = attrs.field()

        return Task

    if maybe_function:
        return decorator(maybe_function)
    else:
        return decorator
