import itertools
import inspect
from pathlib import Path
import os
import sys
import typing as ty
import attr
from ..engine.specs import (
    LazyField,
    StateArray,
    MultiInputObj,
    MultiOutputObj,
)

try:
    from typing import get_origin, get_args
except ImportError:
    # Python < 3.8
    from typing_extensions import get_origin, get_args  # type: ignore


NO_GENERIC_ISSUBCLASS = sys.version_info.major == 3 and sys.version_info.minor < 10

if NO_GENERIC_ISSUBCLASS:
    from typing_utils import issubtype

try:
    import numpy
except ImportError:
    HAVE_NUMPY = False
else:
    HAVE_NUMPY = True

T = ty.TypeVar("T")
TypeOrAny = ty.Union[type, ty.Any]


class TypeParser(ty.Generic[T]):
    """A callable which can be used as a converter for attrs.fields to check whether an
    object or LazyField matches the specified field type, or can be
    coerced into it (given the criteria passed on initialisation of the checker).
    Nested container type are expanded and each of their type args are checked/coerced
    against corresponding parts of the object.

    Parameters
    ----------
    tp : type
        the type objects will be coerced to
    coercible: Iterable[ty.Tuple[type or Any, type or Any]], optional
        limits coercing between the pairs of types where they appear within the
        tree of more complex nested container types. If None, then all types are
        coercible except explicitly excluded
    not_coercible: Iterable[ty.Tuple[type or Any, type or Any]], optional
        excludes the limits coercing between the pairs of types where they appear within
        the tree of more complex nested container types. Overrides 'coercible' to enable
        you to carve out exceptions, such as TypeParser(list, coercible=[(ty.Iterable, list)],
        not_coercible=[(str, list)])
    """

    tp: ty.Type[T]
    coercible: ty.List[ty.Tuple[TypeOrAny, TypeOrAny]]
    not_coercible: ty.List[ty.Tuple[TypeOrAny, TypeOrAny]]

    COERCIBLE_DEFAULT: ty.Tuple[ty.Tuple[type, type], ...] = (
        (ty.Sequence, ty.Sequence),
        (ty.Mapping, ty.Mapping),
        (Path, os.PathLike),
        (str, os.PathLike),
        (os.PathLike, Path),
        (os.PathLike, str),
        (ty.Any, MultiInputObj),
        (int, float),
    )
    if HAVE_NUMPY:
        COERCIBLE_DEFAULT += (
            (numpy.integer, int),
            (numpy.floating, float),
            (numpy.bool_, bool),
            (numpy.integer, float),
            (numpy.character, str),
            (numpy.complexfloating, complex),
            (numpy.bytes_, bytes),
        )

    NOT_COERCIBLE_DEFAULT = ((str, ty.Sequence), (ty.Sequence, str))

    def __init__(
        self,
        tp,
        coercible: ty.Optional[
            ty.Iterable[ty.Tuple[TypeOrAny, TypeOrAny]]
        ] = COERCIBLE_DEFAULT,
        not_coercible: ty.Optional[
            ty.Iterable[ty.Tuple[TypeOrAny, TypeOrAny]]
        ] = NOT_COERCIBLE_DEFAULT,
    ):
        def expand_pattern(t):
            """Recursively expand the type arguments of the target type in nested tuples"""
            if t is inspect._empty:
                return None
            origin = get_origin(t)
            if origin is None:
                return t
            args = get_args(t)
            if not args or args == (Ellipsis,):
                assert isinstance(origin, type)
                return origin
            return (origin, [expand_pattern(a) for a in args])

        self.tp = tp
        self.coercible = (
            list(coercible) if coercible is not None else [(ty.Any, ty.Any)]
        )
        self.not_coercible = list(not_coercible) if not_coercible is not None else []
        self.pattern = expand_pattern(tp)

    def __call__(self, obj: ty.Any) -> ty.Union[T, LazyField[T]]:
        """Attempts to coerce the object to the specified type, unless the value is
        a LazyField where the type of the field is just checked instead or an
        attrs.NOTHING where it is simply returned.

        Parameters
        ----------
        obj : ty.Any
            the object to coerce/check-type

        Returns
        -------
        T
            the coerced object

        Raises
        ------
        TypeError
            if the coercion is not possible, or not specified by the
            `coercible`/`not_coercible` parameters, then a TypeError is raised
        """
        coerced: T
        if obj is attr.NOTHING:
            coerced = attr.NOTHING  # type: ignore[assignment]
        elif isinstance(obj, LazyField):
            self.check_type(obj.type)
            coerced = obj
        elif isinstance(obj, StateArray):
            coerced = StateArray(self(o) for o in obj)  # type: ignore[assignment]
        else:
            coerced = self.coerce(obj)
        return coerced

    def coerce(self, object_: ty.Any) -> T:
        """Attempts to coerce the given object to the type of the specified type"""
        if self.pattern is None:
            return object_

        def expand_and_coerce(obj, pattern: ty.Union[type, tuple]):
            """Attempt to expand the object along the lines of the coercion pattern"""
            if obj is attr.NOTHING:
                return attr.NOTHING
            if not isinstance(pattern, tuple):
                return coerce_basic(obj, pattern)
            origin, pattern_args = pattern
            if origin is ty.Union:
                return coerce_union(obj, pattern_args)
            if not self.is_instance(obj, origin):
                self.check_coercible(obj, origin)
                type_ = origin
            else:
                type_ = type(obj)
            if issubclass(type_, ty.Mapping):
                return coerce_mapping(obj, type_, pattern_args)
            try:
                obj_args = list(obj)
            except TypeError as e:
                msg = (
                    f" (part of coercion from {object_} to {self.pattern}"
                    if obj is not object_
                    else ""
                )
                raise TypeError(
                    f"Could not coerce to {type_} as {obj} is not iterable{msg}"
                ) from e
            if issubclass(origin, ty.Tuple):
                return coerce_tuple(type_, obj_args, pattern_args)
            return coerce_sequence(type_, obj_args, pattern_args)

        def coerce_basic(obj, pattern):
            """Coerce an object to a "basic types" like `int`, `float`, `bool`, `Path`
            and `File` in contrast to compound types like `list[int]`,
            `dict[str, str]` and `dict[str, list[int]]`"""
            if self.is_instance(obj, pattern):
                return obj
            self.check_coercible(obj, pattern)
            return coerce_to_type(obj, pattern)

        def coerce_union(obj, pattern_args):
            """Coerce an object into the first type in a Union construct that it is
            coercible into"""
            reasons = []
            for arg in pattern_args:
                try:
                    return expand_and_coerce(obj, arg)
                except TypeError as e:
                    reasons.append(e)
            raise TypeError(
                f"Could not coerce object, {obj}, to any of the union types {pattern_args}:\n\n"
                + "\n\n".join(f"{a} -> {e}" for a, e in zip(pattern_args, reasons))
            )

        def coerce_mapping(
            obj: ty.Mapping, type_: ty.Type[ty.Mapping], pattern_args: list
        ):
            """Coerce a mapping (e.g. dict)"""
            key_pattern, val_pattern = pattern_args
            items: ty.Iterable[ty.Tuple[ty.Any, ty.Any]]
            try:
                items = obj.items()
            except AttributeError as e:
                msg = (
                    f" (part of coercion from {object_} to {self.pattern}"
                    if obj is not object_
                    else ""
                )
                raise TypeError(
                    f"Could not coerce to {type_} as {obj} is not a mapping type{msg}"
                ) from e
            return coerce_to_type(
                {
                    expand_and_coerce(k, key_pattern): expand_and_coerce(v, val_pattern)
                    for k, v in items
                },
                type_,
            )

        def coerce_tuple(
            type_: ty.Type[ty.Sequence],
            obj_args: list,
            pattern_args: list,
        ):
            """coerce to a tuple object"""
            if pattern_args[-1] is Ellipsis:
                pattern_args = itertools.chain(  # type: ignore[assignment]
                    pattern_args[:-2], itertools.repeat(pattern_args[-2])
                )
            elif len(pattern_args) != len(obj_args):
                raise TypeError(
                    f"Incorrect number of items in tuple, expected "
                    f"{len(pattern_args)}, got {len(obj_args)}"
                )
            return coerce_to_type(
                [expand_and_coerce(o, p) for o, p in zip(obj_args, pattern_args)], type_
            )

        def coerce_sequence(
            type_: ty.Type[ty.Sequence], obj_args: list, pattern_args: list
        ):
            """Coerce a non-tuple sequence object (e.g. list, ...)"""
            assert len(pattern_args) == 1
            return coerce_to_type(
                [expand_and_coerce(o, pattern_args[0]) for o in obj_args], type_
            )

        def coerce_to_type(obj, type_):
            """Attempt to do the innermost (i.e. non-nested) coercion and fail with
            helpful message
            """
            try:
                return type_(obj)
            except (TypeError, ValueError) as e:
                msg = (
                    f" (part of coercion from {object_} to {self.pattern}"
                    if obj is not object_
                    else ""
                )
                raise TypeError(f"Cannot coerce {obj} into {type_}{msg}") from e

        return expand_and_coerce(object_, self.pattern)

    def check_type(self, type_: ty.Type[ty.Any]):
        """Checks the given type to see whether it matches or is a subtype of the
        specified type or whether coercion rule is specified between the types

        Parameters
        ----------
        type_ : ty.Type[ty.Any]
            the type to check whether it is coercible into the specified type

        Raises
        ------
        TypeError
            if the type is not either the specified type, a sub-type or coercible to it
        """
        if self.pattern is None or type_ is ty.Any:
            return
        if self.is_subclass(type_, StateArray):
            args = get_args(type_)
            if not args:
                raise TypeError("StateArrays without any type arguments are invalid")
            if len(args) > 1:
                raise TypeError(
                    f"StateArrays with more than one type argument ({args}) are invalid"
                )
            return self.check_type(args[0])

        def expand_and_check(tp, pattern: ty.Union[type, tuple]):
            """Attempt to expand the object along the lines of the coercion pattern"""
            if not isinstance(pattern, tuple):
                return check_basic(tp, pattern)
            pattern_origin, pattern_args = pattern
            if pattern_origin is ty.Union:
                return check_union(tp, pattern_args)
            tp_origin = get_origin(tp)
            if tp_origin is None:
                if issubclass(tp, pattern_origin):
                    raise TypeError(
                        f"Type {tp} wasn't declared with type args required to match pattern "
                        f"{pattern_args}, when matching {type_} to {self.pattern}"
                    )
                raise TypeError(
                    f"{tp} doesn't match pattern {pattern}, when matching {type_} to "
                    f"{self.pattern}"
                )
            tp_args = get_args(tp)
            self.check_coercible(tp_origin, pattern_origin)
            if issubclass(pattern_origin, ty.Mapping):
                return check_mapping(tp_args, pattern_args)
            if issubclass(pattern_origin, ty.Tuple):
                if not issubclass(tp_origin, ty.Tuple):
                    assert len(tp_args) == 1
                    tp_args += (Ellipsis,)
                return check_tuple(tp_args, pattern_args)
            return check_sequence(tp_args, pattern_args)

        def check_basic(tp, pattern):
            if not self.is_subclass(tp, pattern):
                self.check_coercible(tp, pattern)

        def check_union(tp, pattern_args):
            if get_origin(tp) is ty.Union:
                for tp_arg in get_args(tp):
                    reasons = []
                    for pattern_arg in pattern_args:
                        try:
                            expand_and_check(tp_arg, pattern_arg)
                        except TypeError as e:
                            reasons.append(e)
                        else:
                            reasons = None
                            break
                    if reasons:
                        raise TypeError(
                            f"Cannot coerce {tp} to ty.Union[{', '.join(pattern_args)}], "
                            f"because {tp_arg} cannot be coerced to any of its args:\n\n"
                            + "\n\n".join(
                                f"{a} -> {e}" for a, e in zip(pattern_args, reasons)
                            )
                        )
                return
            reasons = []
            for pattern_arg in pattern_args:
                try:
                    return expand_and_check(tp, pattern_arg)
                except TypeError as e:
                    reasons.append(e)
            raise TypeError(
                f"Cannot coerce {tp} to any of the union types:\n\n"
                + "\n\n".join(f"{a} -> {e}" for a, e in zip(pattern_args, reasons))
            )

        def check_mapping(tp_args, pattern_args):
            key_pattern, val_pattern = pattern_args
            key_tp, val_tp = tp_args
            expand_and_check(key_tp, key_pattern)
            expand_and_check(val_tp, val_pattern)

        def check_tuple(tp_args, pattern_args):
            if pattern_args[-1] is Ellipsis:
                if len(pattern_args) == 1:  # matches anything
                    return
                if tp_args[-1] is Ellipsis:
                    return expand_and_check(tp_args[0], pattern_args[0])
                for arg in tp_args:
                    expand_and_check(arg, pattern_args[0])
                return
            if len(tp_args) != len(pattern_args):
                raise TypeError(
                    f"Wrong number of type arguments in tuple {tp_args}  compared to pattern "
                    f"{pattern_args} in attempting to match {type_} to {self.pattern}"
                )
            for t, p in zip(tp_args, pattern_args):
                expand_and_check(t, p)

        def check_sequence(tp_args, pattern_args):
            assert len(pattern_args) == 1
            if tp_args[-1] is Ellipsis:
                tp_args = tp_args[:-1]
                if not tp_args:
                    raise TypeError(
                        "Generic ellipsis type arguments not specific enough to match "
                        f"{pattern_args} in attempting to match {type_} to {self.pattern}"
                    )
            for arg in tp_args:
                expand_and_check(arg, pattern_args[0])

        return expand_and_check(type_, self.pattern)

    def check_coercible(
        self, source: ty.Union[object, type], target: ty.Union[type, ty.Any]
    ):
        """Checks whether the source object or type is coercible to the target type
        given the coercion rules defined in the `coercible` and `not_coercible` attrs

        Parameters
        ----------
        source : object or type
            source object or type to be coerced
        target : type or ty.Any
            target type for the source to be coerced to

        Raises
        ------
        TypeError
            If the source type cannot be coerced into the target type depending on the
            explicit inclusions and exclusions set in the `coercible` and `not_coercible`
            member attrs
        """
        if source is target:
            return
        source_origin = get_origin(source)
        if source_origin is not None:
            source = source_origin

        source_check = self.is_subclass if inspect.isclass(source) else self.is_instance

        def matches(criteria):
            return [
                (src, tgt)
                for src, tgt in criteria
                if source_check(source, src) and self.is_subclass(target, tgt)
            ]

        def type_name(t):
            try:
                return t.__name__
            except AttributeError:
                return t._name  # typing generics for Python < 3.10

        if not matches(self.coercible):
            raise TypeError(
                f"Cannot coerce {repr(source)} into {target} as the coercion doesn't match "
                f"any of the explicit inclusion criteria: "
                + ", ".join(
                    f"{type_name(s)} -> {type_name(t)}" for s, t in self.coercible
                )
            )
        matches_not_coercible = matches(self.not_coercible)
        if matches_not_coercible:
            raise TypeError(
                f"Cannot coerce {repr(source)} into {target} as it is explicitly "
                "excluded by the following coercion criteria: "
                + ", ".join(
                    f"{type_name(s)} -> {type_name(t)}"
                    for s, t in matches_not_coercible
                )
            )

    @classmethod
    def matches(
        cls,
        obj: ty.Type[ty.Any],
        target: ty.Type[ty.Any],
        coercible: ty.Optional[ty.List[ty.Tuple[TypeOrAny, TypeOrAny]]] = None,
        not_coercible: ty.Optional[ty.List[ty.Tuple[TypeOrAny, TypeOrAny]]] = None,
    ) -> bool:
        """Returns true if the provided type matches the pattern of the TypeParser

        Parameters
        ----------
        type_ : type
            the type to check
        target : type
            the target type to check against
        coercible: list[tuple[type, type]], optional
            determines the types that can be automatically coerced from one to the other, e.g. int->float
        not_coercible: list[tuple[type, type]], optional
            explicitly excludes some coercions from the coercible list,
            e.g. str -> Sequence where coercible includes Sequence -> Sequence

        Returns
        -------
        matches : bool
            whether the type matches the target type factoring in sub-classes and coercible
            pairs
        """
        if coercible is None:
            coercible = []
        if not_coercible is None:
            not_coercible = []
        parser = cls(target, coercible=coercible, not_coercible=not_coercible)
        try:
            parser.coerce(obj)
        except TypeError:
            return False
        return True

    @classmethod
    def matches_type(
        cls,
        type_: ty.Type[ty.Any],
        target: ty.Type[ty.Any],
        coercible: ty.Optional[ty.List[ty.Tuple[TypeOrAny, TypeOrAny]]] = None,
        not_coercible: ty.Optional[ty.List[ty.Tuple[TypeOrAny, TypeOrAny]]] = None,
    ) -> bool:
        """Returns true if the provided type matches the pattern of the TypeParser

        Parameters
        ----------
        type_ : type
            the type to check
        target : type
            the target type to check against
        coercible: list[tuple[type, type]], optional
            determines the types that can be automatically coerced from one to the other, e.g. int->float
        not_coercible: list[tuple[type, type]], optional
            explicitly excludes some coercions from the coercible list,
            e.g. str -> Sequence where coercible includes Sequence -> Sequence

        Returns
        -------
        matches : bool
            whether the type matches the target type factoring in sub-classes and coercible
            pairs
        """
        if coercible is None:
            coercible = []
        if not_coercible is None:
            not_coercible = []
        parser = cls(target, coercible=coercible, not_coercible=not_coercible)
        try:
            parser.check_type(type_)
        except TypeError:
            return False
        return True

    @staticmethod
    def is_instance(
        obj: object, candidates: ty.Union[ty.Type[ty.Any], ty.Iterable[ty.Type[ty.Any]]]
    ) -> bool:
        """Checks whether the object is an instance of cls or that cls is typing.Any,
        extending the built-in isinstance to check nested type args

        Parameters
        ----------
        obj: object
            the object to check whether it is an instance of one of the candidates
        candidates : type or ty.Iterable[type]
            the candidate types to check the object against
        """
        if not isinstance(candidates, ty.Iterable):
            candidates = [candidates]
        for candidate in candidates:
            if candidate is ty.Any:
                return True
            if NO_GENERIC_ISSUBCLASS:
                if candidate is type and inspect.isclass(obj):
                    return True
                if issubtype(type(obj), candidate) or (
                    type(obj) is dict and candidate is ty.Mapping
                ):
                    return True
            else:
                if isinstance(obj, candidate):
                    return True
        return False

    @classmethod
    def is_subclass(
        cls,
        klass: ty.Type[ty.Any],
        candidates: ty.Union[ty.Type[ty.Any], ty.Iterable[ty.Type[ty.Any]]],
        any_ok: bool = False,
    ) -> bool:
        """Checks whether the class a is either the same as b, a subclass of b or b is
        typing.Any, extending built-in issubclass to check nested type args

        Parameters
        ----------
        klass : type
            the klass to check whether it is a subclass of one of the candidates
        candidates : type or ty.Iterable[type]
            the candidate types to check the object against
        any_ok : bool
            whether klass=typing.Any should return True or False
        """
        if not isinstance(candidates, ty.Iterable):
            candidates = [candidates]

        for candidate in candidates:
            if NO_GENERIC_ISSUBCLASS:
                if klass is type and candidate is not type:
                    return False
                if issubtype(klass, candidate) or (
                    klass is dict and candidate is ty.Mapping
                ):
                    return True
            else:
                if klass is ty.Any:
                    if ty.Any in candidates:
                        return True
                    else:
                        return any_ok
                origin = get_origin(klass)
                if origin is ty.Union:
                    args = get_args(klass)
                    if get_origin(candidate) is ty.Union:
                        candidate_args = get_args(candidate)
                    else:
                        candidate_args = [candidate]
                    return all(
                        any(cls.is_subclass(a, c) for a in args) for c in candidate_args
                    )
                if origin is not None:
                    klass = origin
                if klass is candidate or candidate is ty.Any:
                    return True
                if issubclass(klass, candidate):
                    return True
        return False

    @classmethod
    def contains_type(cls, target: ty.Type[ty.Any], type_: ty.Type[ty.Any]):
        """Checks a potentially nested type for sub-classes of the target type

        Parameters
        ----------
        target : type
            the target type to check for sub-classes of
        type_: type
            the type to check for nested types that are sub-classes of target
        """
        if cls.is_subclass(type_, target):
            return True
        if type_ in (str, bytes, int, bool, float):  # shortcut primitive types
            return False
        type_args = get_args(type_)
        if not type_args:
            return False
        type_origin = get_origin(type_)
        if type_origin is ty.Union:
            for type_arg in type_args:
                if cls.contains_type(target, type_arg):
                    return True
            return False
        if cls.is_subclass(type_origin, ty.Mapping):
            type_key, type_val = type_args
            return cls.contains_type(target, type_key) or cls.contains_type(
                target, type_val
            )
        if cls.is_subclass(type_, (ty.Sequence, MultiOutputObj)):
            if type_args[-1] == Ellipsis:
                type_args = type_args[:-1]
            return any(cls.contains_type(target, a) for a in type_args)
        return False

    @classmethod
    def apply_to_instances(
        cls,
        target_type: ty.Type[ty.Any],
        func: ty.Callable,
        value: ty.Any,
        cache: ty.Optional[ty.Dict[int, ty.Any]] = None,
    ) -> ty.Any:
        """Applies a function to all instances of the given type that are potentially
        nested within the given value, caching previously computed modifications to
        handle repeated elements

        Parameters
        ----------
        target_type : type
            the target type to apply the function to
        func : callable
            the callable object (e.g. function) to apply to the instances
        value : Any
            the value to copy files from (if required)
        cache: dict, optional
            guards against multiple references to the same objects by keeping a cache of
            the modified
        """
        if (
            not cls.is_instance(value, (target_type, ty.Mapping, ty.Sequence))
            or target_type is not str
            and cls.is_instance(value, str)
        ):
            return value
        if cache is None:
            cache = {}
        obj_id = id(value)
        try:
            return cache[obj_id]
        except KeyError:
            pass
        if cls.is_instance(value, target_type):
            modified = func(value)
        elif cls.is_instance(value, ty.Mapping):
            modified = type(value)(  # type: ignore
                (
                    cls.apply_to_instances(target_type, func, key),
                    cls.apply_to_instances(target_type, func, val),
                )
                for (key, val) in value.items()
            )
        else:
            assert cls.is_instance(value, (ty.Sequence, MultiOutputObj))
            args = [cls.apply_to_instances(target_type, func, val) for val in value]
            modified = type(value)(args)  # type: ignore
        cache[obj_id] = modified
        return modified

    @classmethod
    def get_item_type(
        cls, sequence_type: ty.Type[ty.Sequence[T]]
    ) -> ty.Union[ty.Type[T], ty.Any]:
        """Return the type of the types of items in a sequence type

        Parameters
        ----------
        sequence_type: type[Sequence]
            the type to find the type of the items of

        Returns
        -------
        item_type: type or None
            the type of the items
        """
        if not TypeParser.is_subclass(sequence_type, ty.Sequence):
            raise TypeError(
                f"Cannot get item type from {sequence_type}, as it is not a sequence type"
            )
        args = get_args(sequence_type)
        if not args:
            return ty.Any
        if len(args) > 1 and not (len(args) == 2 and args[-1] == Ellipsis):
            raise TypeError(
                f"Cannot get item type from {sequence_type}, as it has multiple "
                f"item types: {args}"
            )
        return args[0]
