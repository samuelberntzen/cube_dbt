from typing import Union

from cube_dbt.column import Column
from cube_dbt.dump import SafeString, dump
from cube_dbt.measure import Measure
from cube_dbt.test import Test


class Model:
    def __init__(self, model_dict: dict) -> None:
        self._model_dict = model_dict
        self._columns = None
        self._measures = None
        self._primary_key = None
        self._tests = []
        pass

    def __repr__(self) -> str:
        return str(self._model_dict)

    def _init_columns(self) -> None:
        if self._columns == None:
            self._columns = list(
                Column(self.name, column)
                for key, column in self._model_dict["columns"].items()
            )
            self._detect_primary_key()

    def _init_measures(self) -> None:
        if self._measures == None:
            measures = self._model_dict["meta"].get("measures", {})
            if measures:
                self._measures = list(
                    Measure(measure) for measure in self._model_dict["meta"]["measures"]
                )
            else:
                self._measures = []

    def _detect_primary_key(self) -> None:
        candidates = list(column for column in self._columns if column.primary_key)

        if len(candidates) > 1:
            column_names = list(column.name for column in candidates)
            raise RuntimeError(
                f"More than one primary key column found in {self.name}: {', '.join(column_names)}"
            )

        self._primary_key = candidates[0] if len(candidates) == 1 else None

    def add_test(self, test: "Test") -> None:
        self._tests.append(test)

    @property
    def name(self) -> str:
        return self._model_dict["name"]

    @property
    def description(self) -> str:
        return self._model_dict["description"]

    @property
    def sql_table(self) -> str:
        if "relation_name" in self._model_dict:
            return self._model_dict["relation_name"]
        else:
            database = self._model_dict["database"]
            schema = self._model_dict["schema"]
            name = (
                self._model_dict["alias"]
                if "alias" in self._model_dict
                else self._model_dict["name"]
            )
            return f"`{database}`.`{schema}`.`{name}`"

    @property
    def columns(self) -> list[Column]:
        self._init_columns()
        return self._columns

    @property
    def tests(self) -> list:
        return self._tests

    @property
    def primary_key(self) -> Union[Column, None]:
        self._init_columns()
        return self._primary_key

    @property
    def measures(self) -> list[Measure]:
        self._init_measures()
        return self._measures

    def column(self, name: str) -> Column:
        self._init_columns()
        return next(column for column in self._columns if column.name == name)

    def measure(self, name: str) -> Measure:
        self._init_measures()
        return next(measure for measure in self._measures if measure.name == name)

    def test(self, name: str) -> Test:
        self._init_tests()
        return next(test for test in self._tests if test.name == name)

    def _as_cube(self) -> dict:
        data = {}
        data["name"] = self.name
        if self.description:
            data["description"] = self.description
        data["sql_table"] = self.sql_table
        return data

    def as_cube(self) -> str:
        """
        For use in Jinja:
        {{ dbt.model('name').as_cube() }}
        """
        return dump(self._as_cube(), indent=4)

    def _as_dimensions(self, skip: list[str] = []) -> list:
        return list(
            column._as_dimension() for column in self.columns if column.name not in skip
        )

    def as_dimensions(self, skip: list[str] = []) -> str:
        """
        For use in Jinja:
        {{ dbt.model('name').as_dimensions(skip=['id']) }}
        """
        dimensions = self._as_dimensions(skip)
        return dump(dimensions, indent=6) if dimensions else SafeString("")

    def _as_joins(self, skip: list[str] = []) -> list:
        return list(test._as_join() for test in self.tests if test.type not in skip)

    def as_joins(self) -> str:
        """
        For use in Jinja:
        {{ dbt.model('name').as_joins(skip=[]) }}
        """
        joins = self._as_joins()
        return dump(joins, indent=6) if joins else SafeString("")

    def _as_measures(self) -> list:
        return list(measure._as_measure() for measure in self.measures)

    def as_measures(self) -> str:
        """
        For use in Jinja:
        {{ dbt.model('name').as_measures() }}
        """
        measures = self._as_measures()
        return dump(measures, indent=6) if measures else SafeString("")
