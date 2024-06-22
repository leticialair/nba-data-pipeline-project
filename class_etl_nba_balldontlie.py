import datetime
import io
import os
from typing import Literal

import boto3
import pandas as pd
import requests
from pandas import DataFrame
from pydantic.dataclasses import dataclass

from utils.definitions import definitions
from utils.treatments import treatments


@dataclass
class NBABallDontLie:
    """
    Classe utilizada para extrair via API, utilizando o BALLDONTLIE, tratar os dados e subí-los no S3.
    """

    api_key: str = os.getenv("API_KEY_BALLDONTLIE")
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

        # Strip em colunas string
        for column in dict_column_type.keys():
            column_type = dict_column_type[column]
            if column_type == "string":
                dataframe[column] = dataframe[column].astype(str).str.strip()

        # Transformando altura_ft em float (caso aplicável)
        if "altura_ft_jogador" in dataframe.columns:
            dataframe["altura_ft_jogador"] = (
                dataframe["altura_ft_jogador"].str.replace("-", ".").astype(float)
            )

        # Definindo os tipos
        dataframe = dataframe.astype(dict_column_type)

        return dataframe

    def enrich(self, dataframe: DataFrame) -> DataFrame:
        """
        Args:
            dataframe: retornado no método treat com os tratamentos básicos.

        Returns:
            DataFrame: dataframe com os dados enriquecidos.
        """

        # Cria uma coluna com a altura em metros (caso aplicável)
        if "altura_ft_jogador" in dataframe.columns:
            dataframe["altura_mts_jogador"] = dataframe["altura_ft_jogador"].apply(
                treatments.feet_to_meters
            )

        # Cria uma coluna com o peso em quilos (caso aplicável)
        if "peso_lbs_jogador" in dataframe.columns:
            dataframe["peso_kg_jogador"] = dataframe["peso_lbs_jogador"].apply(
                treatments.lbs_to_kg
            )

        return dataframe

    def load(
        self,
        dataframe: DataFrame,
        endpoint: Literal["teams", "players", "games", "stats", "season_averages"],
        camada: Literal["bronze", "silver", "gold"],
        format: Literal["csv", "parquet"],
        region: str = os.getenv("AWS_REGION"),
        account_id: str = os.getenv("AWS_ACCOUNT_ID"),
        access_id: str = os.getenv("AWS_ACCESS_KEY_ID"),
        secret_id: str = os.getenv("AWS_SECRET_ACCESS_KEY"),
    ) -> None:
        """
        Args:
            dataframe: retornado no método enrich com o enriquecimento realizado.
            endpoint: variável utilizada para complementar a url_base e, nesta fase, para identificar o arquivo no S3.
            camada: especificação bucket em que o arquivo será inputado.
            format: formato em que o arquivo será inputado.
            region: região utilizada da AWS.
            account_id: ID da conta utilizada da AWS.
            access_id: ID público da conta utilizada da AWS.
            secret_id: ID secreto da conta utilizada da AWS.
        """

        bucket = f"{camada}-{region}-{account_id}"
        today_id = datetime.datetime.today().strftime("%Y%m%d")
        key = f"nba_balldontlie/{endpoint}/{endpoint}_{today_id}"

        if format == "csv":
            buffer = io.StringIO()
            dataframe.to_csv(buffer, index=False)
            key = f"{key}.csv"

        elif format == "parquet":
            buffer = io.BytesIO()
            dataframe.to_parquet(buffer, index=False)
            key = f"{key}.parquet"

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_id,
            region_name=region,
        )

        s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


if __name__ == "__main__":
    # Fase 1: API to Bronze

    # Teams
    dataframe_teams = NBABallDontLie().extract("teams")
    NBABallDontLie().load(dataframe_teams, "teams", "bronze", "csv")

    # Players
    dataframe_players = NBABallDontLie().extract("players")
    NBABallDontLie().load(dataframe_teams, "players", "bronze", "csv")

    # Fase 2: Bronze to Silver

    # Teams
    dataframe_teams = NBABallDontLie().treat(
        dataframe_teams,
        definitions.dict_teams_column_name,
        definitions.dict_teams_column_type,
    )
    NBABallDontLie().load(dataframe_teams, "teams", "silver", "csv")

    # Players
    dataframe_players = NBABallDontLie().treat(
        dataframe_players,
        definitions.dict_players_column_name,
        definitions.dict_players_column_type,
    )
    NBABallDontLie().load(dataframe_teams, "players", "silver", "csv")

    # Fase 3: Silver to Gold

    # Teams
    dataframe_teams = NBABallDontLie().enrich(dataframe_teams)
    NBABallDontLie().load(dataframe_teams, "teams", "gold", "parquet")

    # Players
    dataframe_players = NBABallDontLie().enrich(dataframe_players)
    NBABallDontLie().load(dataframe_teams, "players", "gold", "parquet")
