# https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html

import os


# ------------------------------------------------------------------------------
# fmt: off
j = [1,
     2,
     3
]
# fmt: on

j = [1, 2, 3]


# ------------------------------------------------------------------------------
# fmt: off
def very_important_function(template: str, *variables, file: os.PathLike, engine: str, header: bool = True, debug: bool = False):
    """Applies `variables` to the `template` and writes to `file`."""
    with open(file, 'w') as f:
        ...
# fmt: on


def very_important_function(
    template: str,
    *variables,
    file: os.PathLike,
    engine: str,
    header: bool = True,
    debug: bool = False
):
    """Applies `variables` to the `template` and writes to `file`."""
    with open(file, "w") as f:
        ...


# ------------------------------------------------------------------------------
# fmt: off
# fmt: on
