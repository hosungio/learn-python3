from enum import Enum


class Type1(str, Enum):
    A = "a"
    B = "b"
    C = "c"


print(Type1.A, Type1.A.name, Type1.A.value)
