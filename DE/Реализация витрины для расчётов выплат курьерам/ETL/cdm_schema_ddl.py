from lib import PgConnect


class CmdSchemaDdl:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_schema(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.srv_wf_settings(
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY, 
    workflow_key varchar NOT NULL UNIQUE, 
    workflow_settings JSON NOT NULL 
); 
 
 
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger( 
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY, 
    courier_id VARCHAR NOT NULL, 
    courier_name TEXT NOT NULL, 
    settlement_year int NOT NULL CHECK (settlement_year > 1900 AND settlement_year < 2500), 
    settlement_month int NOT NULL CHECK (settlement_month >= 1 AND settlement_month <= 12), 
    orders_count int NOT NULL DEFAULT 0 CHECK (orders_count >= 0), 
    orders_total_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0), 
    rate_avg numeric(19, 5) NOT NULL DEFAULT 0 CHECK (rate_avg >= 0), 
    order_processing_fee numeric(19, 5) NOT NULL DEFAULT 0 CHECK (order_processing_fee = orders_total_sum * 0.25), 
    courier_order_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0), 
    courier_tips_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0), 
    courier_reward_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_reward_sum = courier_order_sum + courier_tips_sum * 0.95) 
); 
CREATE INDEX IF NOT EXISTS IDX_dm_courier_ledger__courier_id_settlement_year ON cdm.dm_courier_ledger (courier_id, settlement_year); 
"""
                )