from typing import Any

import numpy as np
import pandas as pd

CATEGORICAL_NUMPY_DTYPES = [np.bool, np.object]
CATEGORICAL_PANDAS_DTYPES = [pd.CategoricalDtype, pd.PeriodDtype]
CATEGORICAL_DTYPES = CATEGORICAL_NUMPY_DTYPES + CATEGORICAL_PANDAS_DTYPES

NUMERICAL_NUMPY_DTYPES = [np.number, np.datetime64]
NUMERICAL_PANDAS_DTYPES = [pd.DatetimeTZDtype]
NUMERICAL_DTYPES = NUMERICAL_NUMPY_DTYPES + NUMERICAL_PANDAS_DTYPES


def is_categorical(dtype: Any) -> bool:
    if is_numerical(dtype):
        return False

    if isinstance(dtype, np.dtype):
        dtype = dtype.type

        return any(issubclass(dtype, c) for c in CATEGORICAL_NUMPY_DTYPES)
    else:
        return any(isinstance(dtype, c) for c in CATEGORICAL_PANDAS_DTYPES)


def is_numerical(dtype: Any) -> bool:
    if isinstance(dtype, np.dtype):
        dtype = dtype.type
        return any(issubclass(dtype, c) for c in NUMERICAL_NUMPY_DTYPES)
    else:
        return any(isinstance(dtype, c) for c in NUMERICAL_PANDAS_DTYPES)

def is_discrete(dtype: Any) -> bool:
    if is_categorical(dtype):
        return True

    assert isinstance(dtype, np.dtype), dtype
    dtype = dtype.type
    return issubclass(dtype, np.integer)

