from cube_dbt.dump import dump


class Measure:
    def __init__(self, measure_dict: dict) -> None:
        self._measure_name = measure_dict["name"]
        self._measure_dict = measure_dict

    def __repr__(self) -> str:
        return str(self._measure_dict)

    @property
    def name(self) -> str:
        return self._measure_dict["name"]

    @property
    def description(self) -> str:
        return self._measure_dict.get("description", None)

    @property
    def type(self) -> str:
        return self._measure_dict["type"]

    @property
    def sql(self) -> str:
        return self._measure_dict.get("sql", None)

    def _as_measure(self) -> dict:
        data = {}
        data["name"] = self.name
        if self.description:
            data["description"] = self.description
        if self.sql:
            data["sql"] = self.sql
        data["type"] = self.type
        return data

    def as_measure(self) -> str:
        """
        For use in Jinja:
        {{ dbt.model('name').measure('name').as_measure() }}
        """
        return dump(self._as_measure(), indent=8)
