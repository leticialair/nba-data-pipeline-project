from pydantic.dataclasses import dataclass
from typing import ClassVar


@dataclass
class Definitions:
    """
    Classe utilizada para inserir definições (dicionários, listas, etc) que serão utilizados em outros métodos.
    """

    dict_teams_column_name: ClassVar[dict] = {
        "id": "id_time",
        "conference": "nome_conferencia",
        "division": "nome_divisao",
        "city": "nome_cidade",
        "name": "nome_time",
        "full_name": "nome_completo_time",
        "abbreviation": "sigla_time",
    }

    dict_teams_column_type: ClassVar[dict] = {
        "id_time": "int32",
        "nome_conferencia": "string",
        "nome_divisao": "string",
        "nome_cidade": "string",
        "nome_time": "string",
        "nome_completo_time": "string",
        "sigla_time": "string",
    }

    dict_players_column_name: ClassVar[dict] = {
        "id": "id_jogador",
        "first_name": "nome_jogador",
        "last_name": "sobrenome_jogador",
        "position": "posicao_jogador",
        "height": "altura_ft_jogador",
        "weight": "peso_pounds_jogador",
        "jersey_number": "num_jersey_jogador",
        "country": "nome_pais_jogador",
        "id": "id_time",
    }

    dict_players_column_type: ClassVar[dict] = {
        "id_jogador": "int32",
        "nome_jogador": "string",
        "sobrenome_jogador": "string",
        "posicao_jogador": "string",
        "altura_ft_jogador": "string",
        "peso_pounds_jogador": "int32",
        "num_jersey_jogador": "int32",
        "nome_pais_jogador": "string",
        "id_time": "int32",
    }


definitions = Definitions()
