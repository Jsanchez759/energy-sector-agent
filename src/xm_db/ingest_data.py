from datetime import datetime, timedelta
from src.xm_db.database_client import DBClient

if __name__ == "__main__":
    db_client = DBClient()
    start_date = datetime.now().date() - timedelta(days=15)
    end_date = datetime.now().date() - timedelta(days=1)

    # Subir a la base de datos por cada tipo de metrica (horaria, mensual y diaria)
    db_client.update_data(start_date, end_date)
    # Actualizar listados
    #db_client.update_list_entities(start_date, end_date)
    # Cerrar conexion
    db_client.close_connection()
