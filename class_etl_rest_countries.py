import datetime
import io
import os
from typing import Literal

import boto3
import pandas as pd
import requests
from pandas import DataFrame
from pydantic.dataclasses import dataclass


@dataclass
class RestCountries:
    """
    Classe utilizada para extrair via API, utilizando o RESTCOUNTRIES, tratar os dados e subí-los no S3.
    """

    def extract(
        self,
    ) -> DataFrame:
        """
        Returns:
            DataFrame: dataframe com os dados retornados.
        """

        url_base = "https://restcountries.com/v3.1/all"
        response = requests.get(url_base)

        if response.status_code != 200:
            raise Exception(f"Erro extraindo dados da API: {response.status_code}")

        data = response.json()
        dataframe = pd.DataFrame(data)

        # A coluna "name" vem com um dicionário em cada linha. Logo, é necessário tratá-la.
        # Crio um dataframe a partir da coluna de dicionários.
        dataframe_team = dataframe["name"].apply(pd.Series)
        # Dou um concat de colunas no dataframe original e removo a coluna "name".
        dataframe = pd.concat([dataframe, dataframe_team], axis=1).drop(
            columns={"name"}
        )

        return dataframe

    def treat(
        self, dataframe: DataFrame, dict_column_name: dict, dict_column_type: dict
    ) -> DataFrame:
        """
        Args:
            dataframe: retornado no método.
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

        pass

    def load(
        self,
        dataframe: DataFrame,
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
        key = f"rest_countries/rest_countries_{today_id}"

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
    dataframe = RestCountries().extract()
