import sqlite3
from utils.logger import logging, CustomException
import sys
import pandas as pd
import warnings
from datetime import datetime, timedelta
from pydataxm.pydataxm import ReadDB

warnings.filterwarnings("ignore")

class XM_API:

    def __init__(self):
        """
        Initializes a new instance of the XM_API Cliente class.
        """

        self.api_object = ReadDB()

    def get_data(
        self, metricId: str, Entity: str, fecha_ini, fecha_fin
    ) -> pd.DataFrame:
        """
        Retrieves data for a specific metric and entity from the XM API.

        Args:
            metricId (str): The ID of the metric to retrieve data for.
            Entity (str): The entity associated with the metric.
            fecha_ini (str): The start date for the data range (format: 'YYYY-MM-DD').
            fecha_fin (str): The end date for the data range (format: 'YYYY-MM-DD').

        Returns:
            pandas.DataFrame: A DataFrame containing the retrieved data.

        Raises:
            utils.CustomException: If an error occurs during the data retrieval process.

        Example:
            >>> api = XM_API()
            >>> data = api.get_data('metric_123', 'entity_abc', '2023-01-01', '2023-01-31')
        """
        try:
            df_data = self.api_object.request_data(
                metricId, Entity, fecha_ini, fecha_fin
            )
            logging.info(
                f"Datos obtenidos de {metricId} de {fecha_ini} hasta {fecha_fin} exitosamente."
            )
            return df_data
        except Exception as err:
            logging.error(f"Exception: {err}")
            return pd.DataFrame
        # raise CustomException(err, sys)

    def get_master_data(self) -> pd.DataFrame:
        """
        Retrieves a list of available metric collections from the XM API.

        Returns:
            list: A list of metric collections.

        Raises:
            CustomException: If an error occurs during the data retrieval process.

        Example:
            >>> api = XM_API()
            >>> collections = api.get_master_data()
        """
        try:
            metrics_collections = self.api_object.get_collections()
            return metrics_collections
        except Exception as err:
            logging.error(f"Exception: {err}")
        raise CustomException(err, sys)


class DBClient:
    def __init__(self, sql_db_path: str = "src/xm_db/dbs/test_xm_data.db"):
        """
        Initializes a new instance of the DB class.

        Args:
            driver (str): The name of the ODBC driver to use.
            server (str): The name or IP address of the SQL Server instance.
            database_name (str): The name of the database to connect to.
        """

        self.conn = sqlite3.connect(sql_db_path)
        self.cursor = self.conn.cursor()
        self.api_client = XM_API()

    def get_connection(self) -> tuple:
        """
        Returns the cursor and connection objects for executing SQL queries.

        Returns:
            tuple: A tuple containing the cursor and connection objects.
        """

        return self.cursor, self.conn

    def insert_data(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Inserts data from a pandas DataFrame into a SQL Server table.

        Args:
            table_name (str): The name of the table to insert data into.
            df (pandas.DataFrame): The DataFrame containing the data to insert.

        Raises:
            CustomException: If an error occurs during the data insertion process.
        """

        try:
            df["Date"] = df["Date"].astype(str)

            rows = [tuple(x) for x in df.values]
            placeholders = ", ".join(["?"] * len(df.columns))
            columns = ", ".join(df.columns)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            self.cursor.executemany(sql, rows)
            self.conn.commit()
            logging.info(f"Datos guardados exitosamente de la metrica {df['id'][0]}.")

        except Exception as err:
            logging.error(f"Exception: {err}")
            raise CustomException(err, sys) from err

    def generate_dates(self, initial_date: datetime, final_date: datetime) -> str:
        """
        Generate a list of dates between two dates.

        Args:
            initial_date (datetime): The starting date from which start the list.
            final_date (datetime): The final date from which stop the list.

        Returns:
            a string which have a list of dates between initial and final date

        Raises:
            CustomException: If an error occurs during the database operation, it raises a CustomException with the original error and context information.
        """
        date_list = []
        delta = timedelta(days=1)
        while initial_date <= final_date:
            date_list.append(f"'{initial_date.strftime('%Y-%m-%d')}'")
            initial_date += delta

        date_string = ", ".join(date_list)
        return date_string

    def delete_last_rows(
        self,
        initial_date: datetime,
        final_date: datetime,
        table_name: str,
        record_id: int,
    ) -> None:
        """
        Delete rows from the specified table for the given metric ID and the specified date and the two preceding dates.

        Args:
            initial_date (datetime): The starting date from which to delete rows.
            table_name (str): The name of the table from which to delete rows.
            record_id (int): The ID of the metric to be deleted.

        Returns:
            None

        Raises:
            CustomException: If an error occurs during the database operation, it raises a CustomException with the original error and context information.
        """
        try:
            string_fechas = self.generate_dates(initial_date, final_date)
            sql = f"DELETE FROM {table_name} WHERE id = {record_id} and date in ({string_fechas})"
            self.conn.execute(sql)
            self.conn.commit()
        except Exception as err:
            logging.error(f"Exception: {err}")
            raise CustomException(err, sys) from err

    def update_list_entities(self, start_date: datetime, end_date: datetime):
        """
        Update all the metrics considered list of entities inside the database.

        Args:
            start_date (datetime): Start date to get the data.
            end_date (datetime): Final date to get the data.

        Returns:
            None

        Raises:
            CustomException: If an error occurs during the database operation, it raises a CustomException with the original error and context information.
        """
        try:
            df_metrics = pd.read_sql(
                "SELECT id, metricId, MetricName, Entity FROM master_table WHERE Type = 'ListsEntities' ",
                self.conn,
            )
            df_metrics = df_metrics[df_metrics["metricId"] != "ListadoMetricas"]
            for index, record in df_metrics.iterrows():
                df_variable = self.api_client.get_data(
                    record["metricId"], record["Entity"], start_date, end_date
                )

                if not df_variable.empty:
                    df_variable = df_variable.fillna("").drop("Id", axis=1)
                    df_variable["id"] = record["id"]
                    df_variable["metricName"] = record["MetricName"]
                    name = record["metricId"]

                    if record["metricId"] == "ListadoRecursos":
                        name = "ListadoRecursos_" + record["Entity"]

                    self.delete_list_entities(record["id"], name)
                    self.insert_data(name, df_variable)
        except Exception as err:
            logging.error(f"Exception: {err}")
            raise CustomException(err, sys) from err

    def delete_list_entities(self, record_id: int, table_name: str) -> None:
        """
        Delete all rows inside entites tables to keep just the last one.

        Args:
            table_name (str): The name of the table from which to delete rows.
            record_id (int): The ID of the metric to be deleted.

        Returns:
            None

        Raises:
            CustomException: If an error occurs during the database operation, it raises a CustomException with the original error and context information.
        """
        try:
            sql = f"DELETE FROM {table_name} WHERE id = {record_id}"
            self.conn.execute(sql)
            self.conn.commit()
        except Exception as err:
            logging.error(f"Exception: {err}")
            raise CustomException(err, sys) from err

    def get_metric_id(self, metric_name: str) -> tuple:
        """
        Retrieves the ID and name of a metric from the master_table.

        Args:
            metric_name (str): The name of the metric to retrieve.

        Returns:
            tuple: A tuple containing the metric ID and name, or (None, None) if not found.

        Raises:
            CustomException: If an error occurs during the data retrieval process.
        """
        try:
            self.cursor.execute(
                "SELECT id, MetricName FROM master_table WHERE metricId = ?",
                metric_name,
            )
            metric_id = self.cursor.fetchone()
            return metric_id[0], metric_id[1] if metric_id else None
        except Exception as err:
            logging.error(f"Exception: {err}")
            raise CustomException(err, sys) from err

    def update_data(self, start_date: datetime, end_date: datetime) -> None:
        """
        Update all the daily, monthly and hourly metrics inside their own tables

        Args:
            start_date (datetime): Start date to get the data.
            end_date (datetime): Final date to get the data.

        Returns:
            None

        Raises:
            CustomException: If an error occurs during the data retrieval process.
        """
        try:
            tables_names = [
                ("hourly_entity", "HourlyEntities"),
                ("monthly_entity", "MonthlyEntities"),
                ("daily_entity", "DailyEntities"),
            ]
            for table_name, entity_type in tables_names:
                # Retrieve metrics for the current entity type
                df_metrics = pd.read_sql(
                    f"SELECT id, metricId, MetricName, Entity FROM master_table WHERE Type = '{entity_type}'",
                    self.conn,
                )

                df_metrics = df_metrics[df_metrics["Entity"] != "Enlace"]

                # Process each record
                for _, record in df_metrics.iterrows():
                    df_variable = self.api_client.get_data(
                        record["metricId"], record["Entity"], start_date, end_date
                    )

                    if not df_variable.empty:
                        df_variable = df_variable.fillna(0).drop(
                            "Id", axis=1, errors="ignore"
                        )

                        # Rename and combine columns based on specific conditions
                        if (
                            "Code" in df_variable.columns
                            or "Name" in df_variable.columns
                        ):
                            df_variable.rename(
                                columns={"Code": "id_recurso", "Name": "id_recurso"},
                                inplace=True,
                            )

                        if table_name == "hourly_entity":
                            if all(
                                col in df_variable.columns
                                for col in ["Values_code", "Values_Name"]
                            ):
                                df_variable["id_recurso"] = (
                                    df_variable["Values_code"]
                                    + " - "
                                    + df_variable["Values_Name"]
                                )
                                df_variable.drop(
                                    ["Values_code", "Values_Name"], axis=1, inplace=True
                                )
                            else:
                                df_variable.rename(
                                    columns={
                                        "Values_code": "id_recurso",
                                        "Values_Name": "id_recurso",
                                    },
                                    inplace=True,
                                )

                            if (
                                "Values_Activity" in df_variable.columns
                                and "Values_Subactivity" in df_variable.columns
                            ):
                                df_variable["id_recurso"] = (
                                    df_variable["Values_Activity"]
                                    + " - "
                                    + df_variable["Values_Subactivity"]
                                )
                                df_variable.drop(
                                    ["Values_Activity", "Values_Subactivity"],
                                    axis=1,
                                    inplace=True,
                                )

                            if "Values_MarketType" in df_variable.columns:
                                df_variable["id_recurso"] += (
                                    " - " + df_variable["Values_MarketType"]
                                )
                                df_variable.drop(
                                    ["Values_MarketType"], axis=1, inplace=True
                                )
                            
                            if "Values_FuelType" in df_variable.columns:
                                df_variable["id_recurso"] += (
                                    " - " + df_variable["Values_FuelType"]
                                )
                                df_variable.drop(
                                    ["Values_FuelType"], axis=1, inplace=True
                                )

                        # Add additional columns
                        df_variable["id"] = record["id"]
                        df_variable["metricName"] = record["MetricName"]
                        self.delete_last_rows(
                            df_variable.Date.min().date(),
                            df_variable.Date.max().date(),
                            table_name,
                            record["id"],
                        )
                        self.insert_data(table_name, df_variable)
        except Exception as err:
            logging.error(f"Exception: {err}")
            raise CustomException(err, sys) from err

    def update_master_table(self, df_new_data: pd.DataFrame) -> None:
        """
        Updates the master_table with new metrics from a DataFrame.

        Args:
            df_new_data (pandas.DataFrame): The DataFrame containing the new metric data.

        Raises:
            CustomException: If an error occurs during the update process.
        """

        try:
            df_master = pd.read_sql_query("SELECT * FROM master_table", self.conn)
            df_master.drop("id", axis=1, inplace=True)
            merged_df = df_new_data.merge(df_master, indicator=True, how="left")
            new_metrics = merged_df[merged_df["_merge"] == "left_only"].drop(
                "_merge", axis=1
            )
            if not new_metrics.empty:
                self.insert_data("master_table", new_metrics)
                logging.info(f"Nuevas metricas añadidas: {new_metrics}")
        except Exception as err:
            logging.error(f"Exception: {err}")
            raise CustomException(err, sys) from err

    def close_connection(self):
        """
        Closes the connection to the SQL Server database.
        """
        self.conn.close()
