get_tables_to_create = """WITH minus_list AS (SELECT 'BIN$' txt FROM dual UNION ALL SELECT 'RN_' txt FROM dual UNION ALL SELECT 'TEMP_' txt FROM dual UNION ALL SELECT 'TMP_' txt FROM dual)

 select owner, table_name, data_type, metadata_yn, metadata_link, responsible_engineer, deleted_yn
 FROM UNI_GRANT_USER.t_db_tables_w_detail  t
 WHERE EXISTS (select 1 from UNI_GRANT_USER.T_DB_USER_CONTACT_INFO t1 WHERE t1.user_type = 'SERVICE' AND t1.contains_master_data = 'Y' AND t.owner = t1.username)
 AND NOT EXISTS (SELECT 1 FROM minus_list WHERE txt = SUBSTR(t.table_name,1,LENGTH(txt))) AND t.metadata_yn = 'N' and t.deleted_yn = 'N' and t.IS_IT_NECCESSARY = 'Y'"""
get_tables_to_update = """SELECT aaa.*, ccc.metadata_link FROM (
  SELECT DISTINCT NVL(aa.owner, bb.owner) owner, NVL(aa.table_name, bb.table_name) table_name 
  FROM (
     SELECT a.owner, a.table_name, b.column_name, b.data_type  
     FROM (
          WITH minus_list AS (SELECT 'BIN$' txt FROM dual UNION ALL SELECT 'RN_' txt FROM dual UNION ALL SELECT 'TEMP_' txt FROM dual UNION ALL SELECT 'TMP_' txt FROM dual) 
          select DISTINCT owner, table_name
          FROM UNI_GRANT_USER.t_db_tables_w_detail  t
          WHERE EXISTS (select 1 from UNI_GRANT_USER.T_DB_USER_CONTACT_INFO t1 WHERE t1.user_type = 'SERVICE' AND t1.contains_master_data = 'Y' AND t.owner = t1.username)
                AND NOT EXISTS (SELECT 1 FROM minus_list WHERE txt = SUBSTR(t.table_name,1,LENGTH(txt))) AND t.metadata_yn = 'Y' and t.deleted_yn = 'N' and t.IS_IT_NECCESSARY = 'Y'
          ) a
     JOIN DBA_TAB_COLUMNS b ON a.owner = b.owner AND a.table_name = b.table_name) aa  
     FULL JOIN T_tables_column_sync bb ON aa.owner = bb.owner AND aa.table_name = bb.table_name AND aa.column_name = bb.column_name AND aa.data_type = bb.data_type
     WHERE aa.column_name IS NULL OR bb.column_name IS NULL)aaa
 JOIN UNI_GRANT_USER.t_db_tables_w_detail ccc ON aaa.owner = ccc.owner AND aaa.table_name= ccc.table_name"""

