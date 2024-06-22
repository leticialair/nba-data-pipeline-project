from pydantic.dataclasses import dataclass


@dataclass
class Treatments:
    """
    Classe utilizada para inserir métodos de tratamento que serão atualizados em demais dataframes.
    """

    def feet_to_meters(self, altura_ft: float) -> float:
        """
        Args:
            altura_ft: altura em pés.

        Returns:
            float: altura em metros.
        """

        return altura_ft * 0.3048

    def pounds_to_kg(self, peso_lbs: int) -> float:
        """
        Args:
            peso_pounds: peso em libras.

        Returns:
            float: peso em quilos.
        """

        return peso_lbs * 0.4536


treatments = Treatments()
