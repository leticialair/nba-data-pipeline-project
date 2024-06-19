import pandas as pd
import requests
from definitions import definitions
from pandas import DataFrame
from pydantic.dataclasses import dataclass
from typing import Literal


@dataclass
class NBABallDontLie:
    """
    Classe utilizada para extrair via API, utilizando o BALLDONTLIE, tratar os dados e subí-los em um banco.
    """

    api_key: str = "56ab1f8c-e9f2-46db-9e65-9e7da5924ab5"
    url_base: str = "https://api.balldontlie.io/v1/"

    def extract(
        self, endpoint: Literal["teams", "players", "games", "stats", "season_averages"]
    ) -> DataFrame:
        """
        Args:
            endpoint: utilizado para complementar a url_base. Os métodos "games", "stats" e "season_averages" só estão
            disponíveis na regular season, que ocorre entre Outubro e Abril.

        Returns:
            DataFrame: dataframe com os dados retornados pelo endpoint escolhido.
        """
        url = self.url_base + endpoint
        headers = {"Authorization": self.api_key}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Erro extraindo dados da API: {response.status_code}")

        data = response.json()["data"]
        dataframe = pd.DataFrame(data)

        # A coluna "team" vem com um dicionário em cada linha no caso do dataframe de players.
        # Logo, é necessário tratá-la.
        if endpoint == "players":
            # Renomeio a id original para que não haja duas colunas com o mesmo nome.
            dataframe = dataframe.rename(columns={"id": "id_jogador"})
            # Crio um dataframe a partir da coluna de dicionários.
            dataframe_team = dataframe["team"].apply(pd.Series)
            # Dou um concat de colunas no dataframe original e removo a coluna "teams".
            dataframe = pd.concat([dataframe, dataframe_team], axis=1).drop(
                columns={"team"}
            )

        return dataframe

    def treat(
        self, dataframe: DataFrame, dict_column_name: dict, dict_column_type: dict
    ) -> DataFrame:
        """
        Args:
            dataframe: retornado no método extract a partir do endpoint escolhido.
            dict_column_name: dicionário com a relação de renomeação das colunas do dataframe escolhdo.
            dict_column_type: dicionário com a relação de tipagem das colunas após renomeação.

        Returns:
            DataFrame: dataframe com os dados tratados (ainda sem enriquecimento).
        """

        # Mantendo apenas as colunas mapeadas
        for column in dataframe.columns:
            if column not in dict_column_name.keys():
                dataframe = dataframe.drop(columns={column})

        # Renomeando as colunas
        dataframe = dataframe.rename(columns=dict_column_name)

        # Definindo os tipos
        dataframe = dataframe.astype(dict_column_type)

        return dataframe

    def enrich(self) -> DataFrame:
        pass

    def load(self) -> DataFrame:
        pass


if __name__ == "__main__":
    dataframe_teams = NBABallDontLie().extract("teams")
    dataframe_players = NBABallDontLie().extract("players")
    dataframe_games = NBABallDontLie().extract("games")
    dataframe_stats = NBABallDontLie().extract("stats")
    dataframe_season_averages = NBABallDontLie().extract("season_averages")

    dict_teams_column_name = definitions.dict_teams_column_name
    dict_teams_column_type = definitions.dict_teams_column_type
    dict_players_column_name = definitions.dict_players_column_name
    dict_players_column_type = definitions.dict_players_column_type

    dataframe_teams = NBABallDontLie().treat(
        dataframe_teams, dict_teams_column_name, dict_teams_column_type
    )
    dataframe_players = NBABallDontLie().treat(
        dataframe_players, dict_players_column_name, dict_players_column_type
    )
