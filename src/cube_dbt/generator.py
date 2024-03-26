import os

from cube_dbt.dbt import Dbt
from cube_dbt.model import Model


class CubeYaml:
    """
    Represents a cube YAML Jinja template for a specified DBT model.
    """

    def __init__(self, model):
        """
        Initializes the CubeYaml class with a model name.

        Parameters:
            model_name (str): The name of the DBT model.
        """
        self.model = model

    def _model_template(self) -> str:
        """
        Generates the model Jinja template part.

        Returns:
            str: The Jinja template part for setting the model.
        """
        return f"{{% set model = dbt_model('{self.model.name}') %}}\n"

    def _cubes_template(self) -> str:
        """
        Generates the cubes Jinja template part.

        Returns:
            str: The Jinja template part for cubes definition.
        """
        return "cubes:\n  - {{ model.as_cube() }}\n"

    def _dimensions_template(self) -> str:
        """
        Generates the dimensions Jinja template part.

        Returns:
            str: The Jinja template part for dimensions definition.
        """

        # Only generate if dimensions has values, e.g. contains non-empty objects
        non_empty_array = [obj for obj in self.model._as_dimensions() if len(obj) > 0]

        if len(non_empty_array) > 0:
            return "    dimensions:\n      {{ model.as_dimensions() }}\n"

        return ""

    def _joins_template(self) -> str:
        """
        Generates the joins Jinja template part.

        Returns:
            str: The Jinja template part for joins definition.
        """
        # Only generate if dimensions has values, e.g. contains non-empty objects
        non_empty_array = [obj for obj in self.model._as_joins() if len(obj) > 0]

        if len(non_empty_array) > 0:
            return "    joins:\n      {{ model.as_joins() }}\n"

        return ""

    def _measures_template(self) -> str:
        # Only generate if dimensions has values, e.g. contains non-empty objects
        non_empty_array = [obj for obj in self.model._as_measures() if len(obj) > 0]

        if len(non_empty_array) > 0:
            return "    measures:\n      {{ model.as_measures() }}\n"

        return ""

    def generate_template(self) -> str:
        """
        Generates the complete cube YAML Jinja template.

        Returns:
            str: The complete Jinja template.
        """
        template_parts = [
            self._model_template(),
            self._cubes_template(),
            self._dimensions_template(),
            self._joins_template(),
            self._measures_template(),
        ]
        return "".join(template_parts)


class CubeGenerator:
    def __init__(self, Dbt: Dbt, schema_path: str):
        self.dbt = Dbt
        self.schema_path = schema_path

    def generate_cubes(self):
        for model in self.dbt.models:
            cube = CubeYaml(model=model)

            # Instantiate template
            template = cube.generate_template()

            # If path does not exist, create it
            if not os.path.exists(f"{self.schema_path}/cubes"):
                os.makedirs(f"{self.schema_path}/cubes")

            with open(f"{self.schema_path}/cubes/{model.name}.yml.jinja", "w") as f:
                f.write(template)
                f.close()
                print(f"Generated cube YAML for {model.name}")
