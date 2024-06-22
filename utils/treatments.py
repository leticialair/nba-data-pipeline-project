from pydantic.dataclasses import dataclass


@dataclass
class Treatments:
    """
    Classe utilizada para inserir métodos de tratamento que serão atualizados em demais dataframes.
    """

    def feet_to_meters(altura_ft: float) -> float:
        """
        Args:
            altura_ft: altura em pés.

        Returns:
            float: altura em metros.
        """

        return altura_ft * 0.3048


treatments = Treatments()
