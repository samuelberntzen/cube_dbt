import logging
import os

from cube_dbt.dbt import Dbt
from cube_dbt.model import Model


class CubeYaml:
    """
    Represents a cube YAML Jinja template for a specified DBT model.

    Attributes:
        model (Model): The DBT model for which the cube YAML template is generated.
    """

    def __init__(self, model: Model):
        """
        Initializes the CubeYaml class with a model.

        Parameters:
            model (Model): An instance of a cube_dbt Dbt model
        """
        self.model = model

    def _model_template(self) -> str:
        """
        Generates the model Jinja template part.

        Returns:
            str: The Jinja template part for setting the model.
        """
        return f"{{% set model = dbt_model('{self.model.name}') %}}\\n"

    def _cubes_template(self) -> str:
        """
        Generates the cubes Jinja template part.

        Returns:
            str: The Jinja template part for cubes definition.
        """
        return "cubes:\\n  - {{ model.as_cube() }}\\n"

    def _dimensions_template(self, dimension_skips: list[str]) -> str:
        """
        Generates the dimensions Jinja template part.

        Args:
            dimension_skips (list): A list of columns or substrings to exclude from dimension definitions.

        Returns:
            str: The Jinja template part for dimensions definition.
        """
        # Only generate if dimensions has values, e.g. contains non-empty objects
        non_empty_array = [obj for obj in self.model._as_dimensions() if len(obj) > 0]

        # Generate skips for dimension
        skip_array = []
        for obj in non_empty_array:
            if any(skip_value in obj["name"] for skip_value in dimension_skips):
                print(f"{obj['name']} should be skipped")
                skip_array.append(obj["name"])

        if len(non_empty_array) > 0:
            return "    dimensions:\\n      {{{{ model.as_dimensions(skip={}) }}}}\\n".format(
                skip_array
            )

        return ""

    def _joins_template(self) -> str:
        """
        Generates the joins Jinja template part.

        Args:
            include (list): A list of test or substrings to include in join definitions.

        Returns:
            str: The Jinja template part for joins definition.
        """
        # Only generate if dimensions has values, e.g. contains non-empty objects
        non_empty_array = [obj for obj in self.model._as_joins() if len(obj) > 0]

        if len(non_empty_array) > 0:
            return "    joins:\\n      {{ model.as_joins() }}\\n"

        return ""

    def _measures_template(self) -> str:
        """
        Generates the measures Jinja template part.

        Returns:
            str: The Jinja template part for measures definition.
        """
        # Only generate if dimensions has values, e.g. contains non-empty objects
        non_empty_array = [obj for obj in self.model._as_measures() if len(obj) > 0]

        if len(non_empty_array) > 0:
            return "    measures:\\n      {{ model.as_measures() }}\\n"

        return ""

    def generate_template(self, dimension_skips: list[str]) -> str:
        """
        Generates the complete cube YAML Jinja template.

        Args:
            dimension_skips (list): A list of columns or substrings to exclude from dimension definitions.

        Returns:
            str: The complete Jinja template.
        """
        template_parts = [
            self._model_template(),
            self._cubes_template(),
            self._dimensions_template(dimension_skips),
            self._joins_template(),
            self._measures_template(),
        ]
        return "".join(template_parts)


class CubeGenerator:
    """
    Generates cube YAML templates for all DBT models.

    example usage:

    ```
    # globals.py

    from cube_dbt import Dbt
    from cube_dbt.generator import CubeGenerator

    dbt = Dbt.from_file("manifest.json", encoding="utf-8")

    generator = CubeGenerator(dbt = dbt, "schema")
    generator.generate_cubes(dimension_skips = ['id', 'primary_key'])
    ```

    Attributes:
        dbt (Dbt): The DBT instance.
        schema_path (str): The path to the schema.
    """

    def __init__(self, Dbt: Dbt, schema_path: str):
        """
        Initializes the CubeGenerator class with a DBT instance and a schema path.

        Parameters:
            Dbt (Dbt): The DBT instance.
            schema_path (str): The path to which Cube stores the schemas, usually "schema"
        """
        self.dbt = Dbt
        self.schema_path = schema_path

    def generate_cubes(self, dimension_skips: list[str] = []):
        """
        Generates cube YAML templates for all DBT models

        Args:
            dimension_skips (list): A list of columns or substrings to exclude from dimension definitions.
        """
        for model in self.dbt.models:
            cube = CubeYaml(model=model)

            # Instantiate template
            template = cube.generate_template(dimension_skips)

            # If path does not exist, create it
            if not os.path.exists(f"{self.schema_path}/cubes"):
                os.makedirs(f"{self.schema_path}/cubes")

            with open(f"{self.schema_path}/cubes/{model.name}.yml.jinja", "w") as f:
                f.write(template)
                f.close()
                logging.info(f"Generated cube for {model.name}")
