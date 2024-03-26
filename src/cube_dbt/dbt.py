import json
import locale
from urllib.request import urlopen

from cube_dbt.model import Model
from cube_dbt.test import Test


class Dbt:
    def __init__(self, manifest: dict) -> None:
        self.manifest = manifest
        self.paths = ""
        self.tags = []
        self.names = []
        self._models = None
        pass

    @staticmethod
    def from_file(manifest_path: str, encoding: str = None) -> "Dbt":
        """Reads a DBT manifest.json file from local path

        Args:
            manifest_path (str): The path to the manifest file, read from the top-level directory of the Cube environment
            encoding (str, optional): Encoding for the manifest.json file. Uses the system locale preferred encoding if not specified.

        Returns:
            Dbt: Dbt manifest class to interact with in Cube
        """
        if encoding is None:
            encoding = locale.getpreferredencoding()
        with open(manifest_path, "r", encoding=encoding) as file:
            manifest = json.loads(file.read())
            return Dbt(manifest)

    @staticmethod
    def from_url(manifest_url: str) -> "Dbt":
        """
        Creates an instance of the Dbt class by loading a JSON manifest from a specified URL.

        Args:
            manifest_url (str): The URL pointing to the JSON manifest file. This URL should be accessible and the file should be in a valid JSON format.

        Returns:
            Dbt: An instance of the Dbt class initialized with the manifest loaded from the given URL.
        """
        with urlopen(manifest_url) as file:
            manifest = json.loads(file.read())
            return Dbt(manifest)

    def filter(
        self, paths: list[str] = [], tags: list[str] = [], names: list[str] = []
    ) -> "Dbt":
        self.paths = paths
        self.tags = tags
        self.names = names
        return self

    def _init_models(self):
        if self._models is None:
            # First, initialize all models without tests
            models_temp = {
                key: Model(node)
                for key, node in self.manifest["nodes"].items()
                if node["resource_type"] == "model"
                and node["config"]["materialized"] != "ephemeral"
                and (
                    any(node["path"].startswith(path) for path in self.paths)
                    if self.paths
                    else True
                )
                and all(tag in node["config"]["tags"] for tag in self.tags)
                and (node["name"] in self.names if self.names else True)
            }

            # Now, iterate over all tests to assign them to their respective models
            for key, node in self.manifest["nodes"].items():
                if node["resource_type"] == "test":
                    test = Test(node)
                    # Each test lists its dependencies on models
                    for dependency in node["depends_on"]["nodes"]:
                        # The dependency is a unique_id that we need to resolve to a model
                        model_unique_id = dependency
                        # Check if the model for this test exists in our temporary models map
                        if model_unique_id in models_temp:
                            models_temp[model_unique_id].add_test(test)

            self._models = list(models_temp.values())

    @property
    def models(self) -> list[Model]:
        self._init_models()
        return self._models

    def model(self, name: str) -> Model:
        self._init_models()
        return next(model for model in self._models if model.name == name)
