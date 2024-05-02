from enum import Enum
from compile.utils import print_conditions


class FactorType(Enum):
    INDICATOR_EXP = 0
    EXPECTATION = 1


class IndicatorExpectation:
    """
    Represents an expectation calculation, accounting for specified conditions and normalizers.
    """

    def __init__(self, denominator_multipliers, conditions, numerator_multipliers=None, spn=None, inverse=False, tables=None):
        self.numerator_multipliers = numerator_multipliers if numerator_multipliers is not None else []
        self.denominator_multipliers = denominator_multipliers
        self.conditions = conditions
        self.spn = spn
        self.min_value = 1 / self.spn.full_join_size if self.spn is not None else 0
        self.inverse = inverse
        self.tables = tables if tables is not None else set()

    def contains_groupby(self, group_bys):
        """ Check if the group-by attributes are part of the conditions. """
        for table, attribute in group_bys:
            if any(cond_table == table and condition.startswith(attribute) for cond_table, condition in self.conditions):
                return True
        return False

    def matches(self, other, ignore_inverse=False, ignore_spn=False):
        """ Compares this object to another IndicatorExpectation to check for equivalence. """
        if not ignore_inverse and self.inverse != other.inverse:
            return False
        if set(self.numerator_multipliers) != set(other.numerator_multipliers):
            return False
        if set(self.denominator_multipliers) != set(other.denominator_multipliers):
            return False
        if set(self.conditions) != set(other.conditions):
            return False
        if not ignore_spn and self.tables != other.tables:
            return False
        return True

    def __hash__(self):
        """ Generate a hash based on the factor's properties. """
        return hash((FactorType.INDICATOR_EXP, self.inverse, frozenset(self.numerator_multipliers),
                     frozenset(self.denominator_multipliers), frozenset(self.conditions), frozenset(self.tables)))

    def is_inverse(self, other):
        """ Check if the other expectation is an inverse of this one. """
        return self.inverse != other.inverse and self.matches(other, ignore_inverse=True)

    def __str__(self):
        """
        String representation of the expectation.
        """
        formula = " * E(" if not self.inverse else " / E("
        formula += "*".join(f"{table}.{norm}" for table, norm in self.numerator_multipliers) or "1"

        if self.denominator_multipliers:
            formula += "/" + "*".join(f"{table}.{norm}" for table, norm in self.denominator_multipliers)

        if self.conditions:
            formula += f" * 1_{{{print_conditions(self.conditions)}}}"
        formula += ")"

        return formula


class Expectation:
    """
    Represents conditional expectation for specific features considering normalizing multipliers.
    """

    def __init__(self, features, normalizing_multipliers, conditions, spn=None):
        self.features = features
        self.normalizing_multipliers = normalizing_multipliers
        self.conditions = conditions
        self.spn = spn
        self.min_value = 1

    def matches(self, other, ignore_spn=False):
        """ Compares this object to another Expectation to check for equivalence. """
        if set(self.features) != set(other.features):
            return False
        if set(self.normalizing_multipliers) != set(other.normalizing_multipliers):
            return False
        if set(self.conditions) != set(other.conditions):
            return False
        if not ignore_spn and self.spn != other.spn:
            return False
        return True

    def __hash__(self):
        """ Generate a hash based on the expectation's properties. """
        return hash((FactorType.EXPECTATION, frozenset(self.features), frozenset(self.normalizing_multipliers),
                     frozenset(self.conditions), self.spn))

    def __str__(self):
        """
        String representation of the conditional expectation.
        """
        feature_str = "*".join(f"{table}.{multiplier}" for table, multiplier in self.features)
        norm_str = "*".join(f"{table}.{norm}" for table, norm in self.normalizing_multipliers)
        condition_str = print_conditions(self.conditions)
        return f"E({feature_str} / ({norm_str}) | {condition_str})"


class Probability:
    """
    Represents the probability calculation for a set of conditions.
    """

    def __init__(self, conditions):
        self.conditions = conditions

    def matches(self, other):
        """ Checks if the conditions match those of another probability object. """
        return set(self.conditions) == set(other.conditions)

    def __str__(self):
        """
        String representation of the probability.
        """
        condition_str = print_conditions(self.conditions)
        return f"P({condition_str})"
