import copy
from enum import Enum


class Table:
    """ Represents a database table with details about keys and relationships. """

    def __init__(self, name, primary_keys=['id'], null_indicator=None, size=1000, csv_path=None,
                 attributes=None, ignored_attributes=None, preserved_foreign_keys=None, sample_rate=1.0,
                 dependency_rules=None, no_compress=None):

        self.name = name
        self.size = size
        self.primary_keys = primary_keys

        self.csv_path = csv_path
        self.attributes = attributes if attributes else []
        self.ignored_attributes = ignored_attributes if ignored_attributes else []
        self.preserved_foreign_keys = preserved_foreign_keys if preserved_foreign_keys else []
        self.no_compress = no_compress if no_compress else []

        self.dependency_rules = [(name + '.' + source, name + '.' + dest) for source, dest in
                                 dependency_rules] if dependency_rules else []

        # Attribute to indicate NULL tuples from FULL OUTER JOINs
        self.null_indicator = null_indicator if null_indicator else name + '_is_null'

        # Foreign key relationships where this table is referenced
        self.incoming_relationships = []

        # Foreign key relationships where this table references other tables
        self.outgoing_relationships = []

        self.sample_rate = sample_rate

    def child_attributes_from_dependency(self, attribute):
        """ Returns child attributes based on functional dependencies where attribute is a parent. """
        return [source for source, dest in self.dependency_rules if dest == attribute]

    def parent_attributes_from_dependency(self, attribute):
        """ Returns parent attributes based on functional dependencies where attribute is a child. """
        return [dest for source, dest in self.dependency_rules if source == attribute]


class Relationship:
    """ Represents a foreign key relationship between two tables. """

    def __init__(self, source_table, target_table, source_attribute, target_attribute, multiplier_name):
        self.source = source_table.name
        self.source_attribute = source_attribute
        self.target = target_table.name
        self.target_attribute = target_attribute
        self.multiplier_name = multiplier_name
        self.multiplier_name_nn = multiplier_name + '_nn'
        self.identifier = f"{self.source}.{self.source_attribute} = {self.target}.{self.target_attribute}"

        source_table.outgoing_relationships.append(self)
        target_table.incoming_relationships.append(self)


class SchemaGraph:
    """ Manages tables and relationships in a schema graph. """

    def __init__(self):
        self.tables = []
        self.relationships = []
        self.table_dict = {}
        self.relationship_dict = {}

    def add_table(self, table):
        self.tables.append(table)
        self.table_dict[table.name] = table

    def add_relationship(self, source, source_attr, target, target_attr, multiplier_name=None):
        multiplier_name = multiplier_name if multiplier_name else f"mul_{source}.{source_attr}"
        relationship = Relationship(self.table_dict[source], self.table_dict[target], source_attr, target_attr, multiplier_name)
        self.relationships.append(relationship)
        self.relationship_dict[relationship.identifier] = relationship
        return relationship.identifier


class QueryType(Enum):
    AQP = 0
    CARDINALITY = 1


class AggregationType(Enum):
    SUM = 0
    AVG = 1
    COUNT = 2


class AggregationOperationType(Enum):
    PLUS = 0
    MINUS = 1
    AGGREGATE = 2


class Query:
    """ Represents a database query, including its conditions, type, and target schema. """

    def __init__(self, schema, query_type=QueryType.CARDINALITY):
        self.schema = schema
        self.query_type = query_type
        self.tables = set()
        self.relationships = set()
        self.table_conditions = {}
        self.conditions = []
        self.aggregations = []
        self.group_by_fields = []

    def add_condition(self, table, condition):
        """ Adds a condition to the query. """
        if table not in self.table_conditions:
            self.table_conditions[table] = []
        self.table_conditions[table].append(condition)
        self.conditions.append((table, condition))

    def add_group_by(self, table, attribute):
        """ Adds a group by clause to the query. """
        self.group_by_fields.append((table, attribute))

    def add_aggregation(self, operation_type, operation=None):
        """ Adds an aggregation operation to the query. """
        self.aggregations.append((operation_type, operation))

    def add_relationship_condition(self, relationship_id):
        """ Adds a join condition based on the relationship identifier. """
        relationship = self.schema.relationship_dict[relationship_id]
        self.tables.add(relationship.source)
        self.tables.add(relationship.target)
        self.relationships.add(relationship_id)

    def copy_for_cardinality(self):
        """ Creates a copy of the current query for cardinality estimation. """
        new_query = Query(self.schema, self.query_type)
        new_query.tables = copy.copy(self.tables)
        new_query.relationships = copy.copy(self.relationships)
        new_query.table_conditions = copy.deepcopy(self.table_conditions)
        new_query.conditions = copy.copy(self.conditions)
        return new_query
