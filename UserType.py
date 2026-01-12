from collections import namedtuple
from enum import Enum
import sys

class UserType(namedtuple('UserType', 'name description'), Enum):
    __order__ = 'S20 A A15 A24 A30 A36 A40 A44 APlus B BSub C CPlus CPlusQuestion D M O S SSub Agent A15Plus A24Plus A30Plus A36Plus A40Plus A44Plus A20Plus A30PlusQuestion'
    S20 = 0, 20
    A = 1, sys.maxsize
    A15 = 2, 15
    A24 = 3, 15
    A30 = 4, 30
    A36 = 5, 36
    A40 = 6, 40
    A44 = 7, 44
    APlus = 19, sys.maxsize
    B = 8, sys.maxsize
    BSub = 9, sys.maxsize
    C = 10, -1
    CPlus = 11, -1
    CPlusQuestion = 12, -1
    D = 13, sys.maxsize
    M = 14, sys.maxsize
    O = 15, sys.maxsize
    S = 16, sys.maxsize
    SSub = 17, sys.maxsize
    Agent = 18, sys.maxsize
    A15Plus = 22, 15
    A24Plus = 23, 15
    A30Plus = 24, 30
    A36Plus = 25, 36
    A40Plus = 26, 40
    A44Plus = 27, 44
    A20Plus = 31, 20
    A30PlusQuestion = 30, 30

    def __str__(self) -> str:
        return str(self.name)