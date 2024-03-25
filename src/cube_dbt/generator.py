import os

from cube_dbt.dbt import Dbt


class CubeYaml:
    """
    Represents a cube YAML Jinja template for a specified DBT model.
    """

    def __init__(self, model_name: str):
        """
        Initializes the CubeYaml class with a model name.

        Parameters:
            model_name (str): The name of the DBT model.
        """
        self.model_name = model_name

    def _model_template(self) -> str:
        """
        Generates the model Jinja template part.

        Returns:
            str: The Jinja template part for setting the model.
        """
        return f"{{% set model = dbt_model('{self.model_name}') %}}\n"

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
        return "    dimensions:\n      {{ model.as_dimensions() }}\n"

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
        ]
        return "".join(template_parts)


class CubeGenerator:
    def __init__(self, Dbt: Dbt, schema_path: str):
        self.dbt = Dbt
        self.schema_path = schema_path

    def generate_cubes(self):
        for model in self.Dbt.models:
            cube = CubeYaml(model_name=model.name)

            # Instantiate template
            template = cube.generate_template()

            # If path does not exist, create it
            if not os.path.exists(f"{self.scehma_path}/cubes"):
                os.makedirs(f"{self.scehma_path}/cubes")

            with open(f"{self.scehma_path}/cubes/{model.name}.yml.jinja", "w") as f:
                f.write(template)
                f.close()
                print(f"Generated cube YAML for {model.name}")
