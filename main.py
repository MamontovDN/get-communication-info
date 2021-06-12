import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def main():
    conn = psycopg2.connect(
        host=os.environ.get("HOST"),
        port=os.environ.get("PORT"),
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
    )
    sql_query = pd.read_sql_query(
        """
            WITH user_sessions AS (
                SELECT s.visitor_id,
                       COUNT(s.visitor_id) row_n,
                       Max(s.date_time)    session_date_time
                FROM sessions s
                         JOIN communications c
                              ON s.visitor_id = c.visitor_id AND
                                 s.site_id = c.site_id
                WHERE s.date_time <= c.date_time
                GROUP BY s.visitor_id)
            SELECT c.communication_id,
                   c.site_id,
                   c.visitor_id,
                   c.date_time communication_date_time,
                   s.visitor_session_id,
                   us.session_date_time,
                   s.campaign_id,
                   us.row_n
            FROM communications c
            LEFT JOIN user_sessions us ON us.visitor_id = c.visitor_id
            LEFT JOIN sessions s ON us.visitor_id = s.visitor_id AND
                                    us.session_date_time = s.date_time
            ;
        """,
        conn,
    )
    df = pd.DataFrame(
        sql_query,
        columns=[
            "communication_id",
            "site_id",
            "visitor_id",
            "communication_date_time",
            "visitor_session_id",
            "session_date_time",
            "campaign_id",
            "row_n",
        ],
    )
    df.to_csv("result_dmitry_mamontov.csv", index=False)
    conn.close()


if __name__ == "__main__":
    main()
