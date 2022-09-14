from datetime import datetime

from typing import Set, Union, Any, Callable


def set_processor(func: Callable, value: Union[set, Any]) -> Any:
    if not isinstance(value, set):
        return value

    return func(value)


def head(value: set) -> Any:
    value = sorting_set(value)
    return value[0]


def get_earlier_date(dates: Set[datetime]) -> datetime:
    return sorting_set(dates)[0]


def set_stringer(value: set) -> str:
    value = sorting_set(value)
    value = [str(v) for v in value]
    is_comma_exist = any([',' in v for v in value])

    join_symbol = '; ' if is_comma_exist else ', '
    return join_symbol.join(value)


def sorting_set(value: set) -> Any:
    value = list(value)
    value.sort()
    return value
