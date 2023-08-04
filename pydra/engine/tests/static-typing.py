import typing as ty
from pathlib import Path
import inspect
from typing import Any
from copy import copy
from typing_extensions import dataclass_transform
import attrs
from fileformats.generic import File, Directory
from pydra.engine.task import TaskBase, FunctionTask, ShellCommandTask
from pydra.engine.core import Result


OutSpec = ty.TypeVar("OutSpec")


class SpecBase(ty.Generic[OutSpec]):
    __pydra_task_class__: ty.Type[TaskBase]
    __pydra_cmd__: ty.Union[str, ty.Callable]

    def __call__(self, name: ty.Optional[str] = None, **kwargs) -> TaskBase:
        """_summary_

        Parameters
        ----------
        name : str, optional
            name of the task, by default the name of the spec interface
        **kwargs
            keyword args passed on to the execution

        Returns
        -------
        Result
            the result of the running the task
        """
        task_kwargs, run_kwargs = self._split_task_kwargs(kwargs)
        task = self.make_task(**task_kwargs)
        return task(**run_kwargs)

    def split(
        self,
        splitter: ty.Union[str, ty.List[str], ty.Tuple[str, ...], None] = None,
        overwrite: bool = False,
        cont_dim: ty.Optional[dict] = None,
        **kwargs,
    ):
        task_kwargs, inputs = self._split_task_kwargs(kwargs)
        task = self.make_task(**kwargs)
        task.split(splitter=splitter, overwrite=overwrite, cont_dim=cont_dim, **inputs)
        return task

    def combine(
        self, combiner: ty.Union[ty.List[str], str], overwrite: bool = False, **kwargs
    ):
        task = self.make_task(**kwargs)
        task.combine(combiner=combiner, overwrite=overwrite, overwrite=overwrite)
        return task

    def make_task(self, **kwargs):
        return self.__pydra_task_class__(self.__pydra_cmd__, inputs=self, **kwargs)

    @classmethod
    def _split_task_kwargs(cls, kwargs):
        kwargs = copy(kwargs)
        task_kwargs = {
            k: kwargs.pop(k)
            for k in kwargs
            if k in inspect.signature(cls.__pydra_task_class__).parameters
        }
        return task_kwargs, kwargs


InSpec = ty.TypeVar("InSpec", bound=SpecBase)


@attrs.define
class LazyOut:
    name: str
    spec: type

    def __getattr___(self, field_name):
        try:
            field = self._fields[field_name]
        except KeyError as e:
            raise AttributeError(
                f"Lazy output interface of {self.name} task does not include "
                f"{field_name}"
            ) from e
        return LazyOutField[field.type](self.name, field_name, field.type)

    @property
    def _fields(self):
        return attrs.fields(self.spec)


@attrs.define
class LazyOutField:
    task_name: str
    field_name: str
    type: type


@attrs.define
class LazyIn:
    name: str
    spec: type

    def __getattr___(self, field_name):
        try:
            field = self._fields[field_name]
        except KeyError as e:
            raise AttributeError(
                f"Lazy output interface of {self.name} task does not include "
                f"{field_name}"
            ) from e
        return LazyInField[field.type](self.name, field_name, field.type)

    @property
    def _fields(self):
        return attrs.fields(self.spec)


@attrs.define
class LazyInField:
    task_name: str
    field_name: str
    type: type


@attrs.define(auto_attribs=False)
class Task(ty.Generic[InSpec, OutSpec]):
    inputs: InSpec = attrs.field()
    name: str = attrs.field()

    @name.default
    def name_default(self):
        return type(self).__name__.lower()

    @property
    def lzout(self) -> OutSpec:
        return ty.cast(OutSpec, LazyOut(self.name, self.inputs.__pydra_output_spec__))


@attrs.define
class Workflow:
    name: str
    tasks: ty.List[Task] = attrs.field(factory=list)
    connections: ty.List[ty.Tuple[str, LazyOutField]] = attrs.field(factory=list)
    input_spec: ty.Dict[str, type] = attrs.field(factory=dict)

    def add(
        self, spec: SpecBase[OutSpec], name: ty.Optional[str] = None
    ) -> Task[SpecBase[OutSpec], OutSpec]:
        task = Task[SpecBase[OutSpec], OutSpec](
            spec, **({"name": name} if name else {})
        )
        self.tasks.append(task)
        return task

    def set_output(self, connection):
        self.connections.append(connection)

    @property
    def lzin(self):
        return LazyIn(self.name, self.input_spec)


def shell_arg(
    default=attrs.NOTHING,
    factory=None,
    argstr="",
    position=None,
    help=None,
    converter=None,
    validator=None,
):
    return attrs.field(
        default=default,
        factory=factory,
        converter=converter,
        validator=validator,
        metadata={"argstr": argstr, "position": position, "help_string": help},
    )


def shell_out(
    default=attrs.NOTHING,
    argstr="",
    position=None,
    help=None,
    filename_template=None,
    callable=None,
    converter=None,
    validator=None,
):
    return attrs.field(
        default=default,
        converter=converter,
        validator=validator,
        metadata={
            "argstr": argstr,
            "position": position,
            "output_file_template": filename_template,
            "help_string": help,
            "callable": callable,
        },
    )


@dataclass_transform(kw_only_default=True, field_specifiers=(shell_arg,))
def shell_task(executable: str):
    def decorator(klass):
        klass.__pydra_cmd__ = executable
        klass.__pydra_task_class__ = ShellCommandTask
        klass.__annotations__["__pydra_executable__"] = str
        return attrs.define(kw_only=True, auto_attrib=True, slots=False)(klass)

    return decorator


@dataclass_transform(kw_only_default=True, field_specifiers=(shell_out,))
def shell_outputs(klass):
    return attrs.define(kw_only=True, auto_attrib=True, slots=False)(klass)


def func_arg(
    default=attrs.NOTHING,
    factory=None,
    help=None,
    converter=None,
    validator=None,
):
    return attrs.field(
        default=default,
        converter=converter,
        validator=validator,
        metadata={"factory": factory, "help_string": help},
    )


@dataclass_transform(kw_only_default=True, field_specifiers=(func_arg,))
def func_task(function: ty.Callable):
    def decorator(klass):
        klass.__pydra_cmd__ = staticmethod(function)
        klass.__pydra_task_class__ = FunctionTask
        nested_out_specs = [
            n for n, s in klass.__dict__.items() if hasattr(s, "__pydra_spec_class__")
        ]
        if not nested_out_specs:
            raise AttributeError(f"No nested output specs found in {klass}")
        elif len(nested_out_specs) > 1:
            raise AttributeError(
                f"More than one output specs found in {klass}: {nested_out_specs}"
            )
        klass.__pydra_output_spec__ = nested_out_specs[0]
        klass.__annotations__["__pydra_cmd__"] = ty.Callable
        return attrs.define(kw_only=True, auto_attrib=False, slots=False)(klass)

    return decorator


def func_out(
    default=attrs.NOTHING,
    help: ty.Optional[str] = None,
    converter=None,
    validator=None,
):
    return attrs.field(
        default=default,
        converter=converter,
        validator=validator,
        metadata={
            "help_string": help,
        },
    )


@dataclass_transform(kw_only_default=True, field_specifiers=(func_out,))
def func_outputs(klass):
    return attrs.define(kw_only=True, auto_attrib=True, slots=False)(klass)


@shell_task("mycmd")
class MyShellSpec(SpecBase["MyShellSpec.Out"]):
    in_file: File = shell_arg(argstr="", position=0)
    an_option: str = shell_arg(
        argstr="--opt",
        position=-1,
        help="an option to flag something",
        converter=str,
    )
    out_file: ty.Optional[ty.Union[Path, str]] = None

    @shell_outputs
    class Out:
        out_file: File = shell_out(
            filename_template="{in_file.stem}_out{in_file.suffix}"
        )
        out_str: str


def func(in_int: int, in_str: str) -> ty.Tuple[int, str]:
    return in_int, in_str


@func_task(func)
class MyFuncSpec(SpecBase["MyFuncSpec.Out"]):
    in_int: int = func_arg(help="a dummy input int")
    in_str: str = func_arg(help="a dummy input int")

    @func_outputs
    class Out:
        out_int: int
        out_str: str


wf = Workflow(name="myworkflow", input_spec={"in_file": File})

mytask = wf.add(MyFuncSpec(in_int=1, in_str="hi"))

mytask2 = wf.add(
    MyFuncSpec(
        in_int=mytask.lzout.out_int,  # should be ok
        in_str=mytask.lzout.out_int,  # should show up as a mypy error
    ),
    name="mytask2",
)

mytask3 = wf.add(
    MyShellSpec(
        in_file=wf.lzin.in_file,
        an_option=mytask2.lzout.out_str,
        out_file="myfile.txt",
    )
)

wf.set_output(("out_str", mytask2.lzout.out_str))
