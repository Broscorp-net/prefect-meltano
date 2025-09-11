import importlib
from typing import Type, TypeVar, Union

T = TypeVar("T", bound=type)


def resolve_class(ref: Union[str, Type[T]], expected_base: type) -> Type[T]:
    """
    Resolve a class reference that may be:
      - a class (subclass of expected_base), or
      - a string in format 'package.module:Class' or 'package.module.Class'.

    Raises:
      TypeError or ValueError on invalid input or if the class is not a subclass
      of expected_base.
    """
    # Already a class?
    if isinstance(ref, type):
        if issubclass(ref, expected_base):
            return ref
        raise TypeError(
            f"Must be a subclass of {expected_base.__module__}.{expected_base.__name__}"
        )

    # String import path
    if isinstance(ref, str):
        if ":" in ref:
            module_name, class_name = ref.split(":", 1)
        else:
            parts = ref.split(".")
            if len(parts) < 2:
                raise ValueError(
                    f"Must be a subclass or import path 'module:Class' or 'module.Class'"
                )
            module_name = ".".join(parts[:-1])
            class_name = parts[-1]

        mod = importlib.import_module(module_name)
        cls = getattr(mod, class_name)
        if not isinstance(cls, type) or not issubclass(cls, expected_base):
            raise TypeError(
                f"{ref!r} does not resolve to a subclass of {expected_base.__module__}.{expected_base.__name__}"
            )
        return cls

    raise TypeError(
        f"Must be a subclass of {expected_base.__module__}.{expected_base.__name__} "
        f"or a string import path"
    )