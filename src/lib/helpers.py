from typing import TYPE_CHECKING
from lib.dsl import *

if TYPE_CHECKING:
    from lib.dsl import Chain



def is_superset(larger: Tuple[Chain], smaller: Tuple[Chain]) -> bool:
    return set(larger).issuperset(set(smaller))


