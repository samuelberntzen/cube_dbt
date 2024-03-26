from cube_dbt.dump import dump


class Test:
    def __init__(self, test_dict: dict) -> None:
        self._test_dict = test_dict

    def __repr__(self) -> str:
        return str(self._test_dict)

    @property
    def name(self) -> str:
        return self._test_dict["name"]

    @property
    def type(self) -> str:
        return self._test_dict["test_metadata"]["name"]

    @property
    def target_model_name(self) -> str:
        return self._test_dict["refs"][0]["name"]

    @property
    def description(self) -> str:
        # Defaulting to an empty string if 'description' is not present
        return self._test_dict.get("description", "")

    @property
    def severity(self) -> str:
        return self._test_dict["config"]["severity"]

    @property
    def tags(self) -> list:
        return self._test_dict["tags"]

    @property
    def refs(self) -> list:
        return self._test_dict["refs"]

    @property
    def meta(self) -> dict:
        return self._test_dict["meta"]

    @property
    def raw_code(self) -> str:
        return self._test_dict["raw_code"]

    @property
    def kwargs(self) -> dict:
        # Extracting 'kwargs' from 'test_metadata' if present, else default to an empty dict
        return self._test_dict.get("test_metadata", {}).get("kwargs", {})

    def as_test_config(self) -> str:
        """
        For use in Jinja:
        {{ dbt.test('name').as_test_config() }}
        """
        data = {
            "name": self.name,
            "description": self.description,
            "severity": self.severity,
            "tags": self.tags,
            "meta": self.meta,
            "raw_code": self.raw_code,
            "kwargs": self.kwargs,
        }
        return dump(data, indent=8)

    def _as_join(self) -> dict:
        data = self._infer_join_from_test()
        return data

    def as_join(self) -> str:
        """
        For use in Jinja:
        {{ dbt.model('name').test('name').as_join() }}
        """
        return dump(self.as_join(), indent=8)

    def _infer_join_from_test(self):
        relationship = None
        for t in self.tags:
            if t in [
                "one_to_many",
                "many_to_one",
                "one_to_one",
                "many_to_many",
            ]:
                relationship = t
                break

        if relationship is not None:
            join_name = self.target_model_name
            from_column = self.kwargs["column_name"]
            to_column = self.kwargs["field"]

            sql = "{{CUBE.{}}} = {{{}.{}}}".format(from_column, join_name, to_column)

            join = {"name": join_name, "sql": sql, "relationship": relationship}

            return join

        return {}
