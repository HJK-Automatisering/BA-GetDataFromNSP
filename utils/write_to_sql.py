import logging
from sqlalchemy import text
from utils.create_dim_df import create_dim_df
from utils.create_ticket_df import create_ticket_df
from utils.get_engine import get_engine

#######################################################################

def write_to_sql(df):
    engine = get_engine()
    agent_groups_df = create_dim_df(df, 'AgentGroup.Id', 'AgentGroup', 'id', 'group')
    task_types_df = create_dim_df(df, 'u_Opgavetype.Id', 'u_Opgavetype', 'id', 'type')
    task_areas_df = create_dim_df(df, 'u_Omrder.Id', 'u_Omrder', 'id', 'area')
    task_status_df = create_dim_df(df, 'BaseEntityStatus.Id', 'BaseEntityStatus', 'id', 'status')
    reasons_for_rejection_df = create_dim_df(df, 'u_Afvisningsrsag.Id', 'u_Afvisningsrsag', 'id', 'reason')
    ticket_df = create_ticket_df(df)

    dim_tables = [
        ('agent_groups', agent_groups_df, 'group'),
        ('task_types', task_types_df, 'type'),
        ('task_areas', task_areas_df, 'area'),
        ('task_status', task_status_df, 'status'),
        ('reasons_for_rejection', reasons_for_rejection_df, 'reason')]

    with engine.begin() as conn:
        for table_name, dim_df, label_col in dim_tables:
            for _, row in dim_df.iterrows():
                conn.execute(
                    text(f'''
                        MERGE {table_name} AS target
                        USING (SELECT :id AS id, :label AS label) AS source
                        ON target.id = source.id
                        WHEN MATCHED THEN
                            UPDATE SET [{label_col}] = source.label
                        WHEN NOT MATCHED THEN
                            INSERT (id, [{label_col}])
                            VALUES (source.id, source.label);
                        '''),
                    {'id': row['id'], 'label': row[label_col]})
            logging.info('%s upserted (%s rows)', table_name, len(dim_df))

        all_cols = list(ticket_df.columns)
        non_id_cols = [c for c in all_cols if c != 'id']
        def col(c):
            return f'[{c}]'
        
        source_select = ', '.join([f':{c} AS {col(c)}' for c in all_cols])
        update_set = ', '.join([f'target.{col(c)} = source.{col(c)}' for c in non_id_cols])
        insert_cols = ', '.join([col(c) for c in all_cols])
        insert_vals = ', '.join([f'source.{col(c)}' for c in all_cols])
        merge_sql = text(
            'MERGE tickets AS target '
            f'USING (SELECT {source_select}) AS source '
            'ON target.id = source.id '
            'WHEN MATCHED THEN '
            f'UPDATE SET {update_set} '
            'WHEN NOT MATCHED THEN '
            f'INSERT ({insert_cols}) '
            f'VALUES ({insert_vals});')

        for _, row in ticket_df.iterrows():
            params = row.to_dict()
            conn.execute(merge_sql, params)
        logging.info('tickets upserted (%s rows)', len(ticket_df))