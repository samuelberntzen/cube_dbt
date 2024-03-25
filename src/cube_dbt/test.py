from cube_dbt.dump import dump


class Test:
    def __init__(self, model_name: str, test_dict: dict) -> None:
        self._model_name = model_name
        self._test_dict = test_dict

    def __repr__(self) -> str:
        return str(self._test_dict)

    @property
    def name(self) -> str:
        return self._test_dict["name"]

    @property
    def kwargs(self) -> str:
        return self._test_dict["kwargs"]

    def _as_join(self) -> dict:
        pass

    def as_join(self) -> str:
        pass
