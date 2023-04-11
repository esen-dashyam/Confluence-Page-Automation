from html import parser
from lxml import etree
from bs4 import BeautifulSoup
import requests
import json
import logging 
import re
import cx_Oracle
import config
from datetime import datetime
import datetime
import urllib.parse
import urllib.request

logging.basicConfig(filename='counter.log',level=logging.INFO,filemode='w')

def body_fetch(id):
    try:
        headers = {
                "Accept": "application/json",
                "Authorization": "Basic ZXJwQHVuaXRlbC5tbjpBOEVYMnExcHR6ZmdFb0xsWTFrdUE0MEE="
            }
        response = requests.request("GET","https://unitelgroup.atlassian.net/wiki/rest/api/content/"+str(id)+"/?&expand=body.storage",headers=headers)
        # print(response.status_code)
        return((response.json()['body']['storage']['value']))
    except Exception as err:
        logging.error('Error Fetching Body at %s: %s', datetime.datetime.now(), err)
def get_id():
    try:
        conn = cx_Oracle.connect(
                config.username,
                config.password,
                config.dsn,
                encoding=config.encoding)
    except Exception as err:
        logging.error('[%s] Error while connecting to database: %s', datetime.datetime.now(), err)
    else: 
        try:
            cur = conn.cursor()
            sql_demo="""SELECT PAGE_ID FROM uni_dynamic_report.Confluence_enforce_table
WHERE IS_DOCUMENTED IS NULL OR IS_DOCUMENTED = 'N'
            """
            cur.execute(sql_demo)
            urls = cur.fetchall()
            ids = [url[0] for url in urls]
            integer_ids=[int(x) for x in ids]
            return integer_ids
        except Exception as err:
            logging.error("[%s] Error executing query: %s", datetime.datetime.now(), err)
# Neg udaa ajiluulah code baisan shvv.Anh vvsgesen documentedsee Y or N gej automatically angilaj chadahgui baisan uchir neg udaadaa iim code bichiv 
def update_documented_list():
    headers = {
        "Accept": "application/json",
        "Authorization": "Basic ZXJwQHVuaXRlbC5tbjpBOEVYMnExcHR6ZmdFb0xsWTFrdUE0MEE="
    }

    # Fetch all page IDs from the database
    conn = cx_Oracle.connect(
        config.username,
        config.password,
        config.dsn,
        encoding=config.encoding)
    cur = conn.cursor()
    try:
        sql_demo = """SELECT PAGE_ID FROM uni_dynamic_report.Confluence_enforce_table"""
        cur.execute(sql_demo)
        urls = cur.fetchall()
        documented_ids = {int(url[0]) for url in urls}
    except Exception as err:
        print("Error executing query", err)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    # Fetch all page IDs from Confluence
    new_id_list = set()
    for number in range(0, 4224, 1):
        response = requests.get("https://unitelgroup.atlassian.net/wiki/rest/api/content/908329019/child/page?limit=1&start=" + str(number), headers=headers)
        results = response.json().get('results', [])
        for result in results:
            new_id_list.add(int(result.get('id', 0)))

    # Update the database with new documented IDs
    conn = cx_Oracle.connect(
        config.username,
        config.password,
        config.dsn,
        encoding=config.encoding)
    cur = conn.cursor()
    try:
        num_updated_docs = 0
        for id in documented_ids:
            if id not in new_id_list:
                cur.execute("UPDATE uni_dynamic_report.Confluence_enforce_table SET IS_DOCUMENTED = 'Y' WHERE PAGE_ID = ?", (id,))
                conn.commit()
                num_updated_docs += 1
        print(f"Updated {num_updated_docs} page IDs in the database.")
    except Exception as err:
        print("Error executing query", err)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def update_engineer():
    # print(body_fetch(916357158))
    # soup = BeautifulSoup(body_fetch(916357158), 'html.parser')
    # data_executor = soup.find('strong', text='Data executor: ').next_sibling.strip()
    # data_creator = soup.find('strong', text='Data creator: ').next_sibling.strip()
    # print("Data executor:", data_executor)
    # print("Data creator:", data_creator)
    conn = cx_Oracle.connect(
                config.username,
                config.password,
                config.dsn,
                encoding=config.encoding)
    cur = conn.cursor()
    sql_demo="""SELECT PAGE_ID FROM uni_dynamic_report.Confluence_enforce_table"""
    cur.execute(sql_demo)
    urls = cur.fetchall()
    ids = [url[0] for url in urls]
    integer_ids=[int(x) for x in ids]
    for id in integer_ids:
        connection = cx_Oracle.connect(
                config.username,
                config.password,
                config.dsn,
                encoding=config.encoding)
        cursor = connection.cursor()
        try :
            pattern_1 = r'<strong>Data executor<\/strong>:\s*([^<]+)'
            pattern_2 = r'<strong>Data creator: </strong>(.+?)</p>'
            matches_1 = re.findall(pattern_1, body_fetch(id))
            matches_2 = re.findall(pattern_2, body_fetch(id))
            query_matches_1 = f"SELECT DISTINCT(EMPLOYEE_EMAIL) FROM uni_hr.hrm_employee WHERE EMPLOYEE_EMAIL='{matches_1[0]}@unitel.mn'"
            cursor.execute(query_matches_1)
            match_email_last_1=cursor.fetchall()
            query_matches_2 = f"SELECT DISTINCT(EMPLOYEE_EMAIL) FROM uni_hr.hrm_employee WHERE EMPLOYEE_EMAIL='{matches_2[0]}@unitel.mn'"
            cursor.execute(query_matches_2)
            match_email_last_2=cursor.fetchall()
            if len(match_email_last_2)>0:
                engineer_update_query = f"UPDATE uni_dynamic_report.Confluence_enforce_table SET NEW_CREATOR = '{matches_2[0]}' WHERE PAGE_ID = {id}"
                cursor.execute(engineer_update_query)
                connection.commit()
            else:
                logging.info("Invalid domain at %s: id=%s", datetime.datetime.now(), id)
            if len(match_email_last_1)>0:
                engineer_update_query = f"UPDATE uni_dynamic_report.Confluence_enforce_table SET NEW_EXECUTOR = '{matches_1[0]}' WHERE PAGE_ID = {id}"
                cursor.execute(engineer_update_query)
                connection.commit()
            else:
                logging.info("Invalid domain at %s: id=%s", datetime.datetime.now(), id)          
        except Exception as err:
              logging.error(f'Error updating engineer {id} at {datetime.datetime.now()}: {err}')
# body_fetch(920322128)
# x=str(body_fetch(916455451))
# start_word_1='Explanation about content'
# end_word_1= 'Related reference data'
# start_index = start_index = x.find(start_word_1) + len(start_word_1)
# end_index = x.find(end_word_1)
# result = x[start_index:end_index].strip()
# print(result)

# def remove_tags(text):
#     clean = re.compile('<.*?>')
#     return re.sub(clean, '', text)
# print(remove_tags(body_fetch(916455451)))

def fetch_latest_date(id):
    try:
        headers = {
                "Accept": "application/json",
                "Authorization": "Basic ZXJwQHVuaXRlbC5tbjpBOEVYMnExcHR6ZmdFb0xsWTFrdUE0MEE="
            }
        response = requests.request("GET","https://unitelgroup.atlassian.net/wiki/rest/api/content/"+str(id),headers=headers)
        # print(response.status_code)
        return((response.json()['version']['when']))
    except Exception as err:
        logging.error('Error Fetching Body Date at %s: %s', datetime.datetime.now(), err)

def update_date():
    connection = cx_Oracle.connect(
        config.username,
        config.password,
        config.dsn,
        encoding=config.encoding)
    cursor = connection.cursor()
    sql_demo="""SELECT PAGE_ID FROM uni_dynamic_report.Confluence_enforce_table"""
    cursor.execute(sql_demo)
    urls = cursor.fetchall()
    ids = [url[0] for url in urls]
    integer_ids=[int(x) for x in ids]
    try:
        for id in integer_ids:
            time = fetch_latest_date(id)
            timestamp = str(time)
            dt_object = datetime.datetime.fromisoformat(timestamp[:-1])
            dt_object_plus_8_hours = dt_object + datetime.timedelta(hours=8)
            formatted_timestamp = dt_object_plus_8_hours.strftime("%Y-%m-%d %H:%M:%S")
            db_update_query = f"UPDATE uni_dynamic_report.Confluence_enforce_table SET UPDATE_DATE = '{formatted_timestamp}' WHERE PAGE_ID = {id}"
            cursor.execute(db_update_query)
            logging.info("Update successful at %s", datetime.datetime.now())
        connection.commit()
    except Exception as err:
        logging.error("Error occurred at %s: %s", datetime.datetime.now(), str(err))
# print(new_string)
# table = etree.HTML(new_string).find("body/table")
# rows = iter(table)
# headers = [col.text for col in next(rows)]
# for row in rows:
#     values = [col.text for col in row]
#     print(dict(zip(headers, values)))

#print(new_string)
# soup = BeautifulSoup(new_string, 'html.parser')
# tbodies = soup.find_all('tbody')
# t_tr=soup.find_all('tr')
# print(s)

        #     ids = []
        #     for url in urls:
        #         try:
        #             id = url[0].split('/')[-2]
        #             ids.append(id)
        #             integer_ids=[int(x) for x in ids]
        #         except Exception as err:
        #             print(f"Error parsing {url}: {err}")
        #             continue 
        #     return integer_ids
        #     # integer_ids=[int(x) for x in ids]
        #     # return(integer_ids)
        # except Exception as err:
        #     print("Error excuting query",err)
# def mark_documented():
#     connection = cx_Oracle.connect(
#                 config.username,
#                 config.password,
#                 config.dsn,
#                 encoding=config.encoding)
#     cursor = connection.cursor()
#     try:
#         for id in get_id():
#             if id not in initial_documented_list():
#         # If the id is not in initial_documented_list, update the corresponding row in the database
#                 cursor.execute("UPDATE uni_dynamic_report.Confluence_enforce_table SET IS_DOCUMENTED = 'Y' WHERE id = :id", id=id)
#                 connection.commit()
#     except Exception as err:
#             print("Error excuting query",err)
#     finally:
#             if connection:
#                 connection.close()

def count_word(id):
    pattern = r'Explanation about content\s*(.*?)\s*<strong>Related reference data'
    try:
        match = re.search(pattern, body_fetch(id))
        substring = match.group(1)
        cleaned_text = re.sub(r'<[^>]*>', '', substring)
        word_list = cleaned_text.split()
        word_count = len(word_list)
    except ValueError:
        word_count = 0
    return word_count-1



# print(get_id())
# print(count_word(216629404))
# experiment = list(map(body_fetch,get_id()))
# print(experiment)
# experiment_count = list(map(count_word,get_id()))
# print(experiment_count)


# def extract_text(id):
#     html_text=body_fetch(id)
#     pattern = r'<p>Position<\/p>(.*)'
#     match = re.search(pattern, html_text, re.DOTALL)
#     if match:
#         return match.group(1)
#     else:
#         return None
def table_counter(id):
    html_text = body_fetch(id)
    pattern = r'<p>Position<\/p>(.*)'
    match = re.search(pattern, html_text, re.DOTALL)
    if match:
        text = match.group(1)
        tr_pattern = r'<tr>(.*?)<\/tr>'
        tr_matches = re.findall(tr_pattern, text, re.DOTALL)
        if tr_matches:
            # strings = []
            # for i, td in enumerate(tr_matches):
            #     if i % 6 == 5:  # check if it is the fifth td in the list
            #         match_1 = re.search(r'<td>(.*?)<\/td>', td)
            #         if match_1:
            #             strings.append(match_1.group(1))
            # return strings
            return tr_matches
    return None


# def extract_strings_from_sixth_td(id):
#     td_list=table_counter(id)
#     strings = []
#     for i, td in enumerate(td_list):
#         if i % 6 == 5:  # check if it is the sixth td in the list
#             match = re.search(r'<td>(.*?)<\/td>', td)
#             if match:
#                 strings.append(match.group(1))
#                 return strings
# print(table_counter(911343667))
# print(list(map(extract_strings_from_sixth_td,get_id())))
# def experiment():
#     conn = cx_Oracle.connect(
#         config.username,
#         config.password,
#         config.dsn,
#         encoding=config.encoding)
#     cur = conn.cursor()
#     tracked_urls=[]

#     for url in urls:
#         parsed_url = urllib.parse.urlparse(url)
#         query = urllib.parse.parse_qs(parsed_url.query)
#         query['utm_source'] = ['confluence']
#         query['utm_medium'] = ['email']
#         query['utm_campaign'] = ['confluence-email']
#         query['utm_content'] = [parsed_url.path.split('/')[-1]]
#         new_query = urllib.parse.urlencode(query, doseq=True)
#         tracked_url = urllib.parse.urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, new_query, parsed_url.fragment))
#         tracked_urls.append(tracked_url)
#         cur.execute("""
#             UPDATE uni_dynamic_report.confluence_enforce_table
#             SET LINK_CLICKED = LINK_CLICKED + 1
#             WHERE METADATA_LINK = %s
#         """, (parsed_url.path.split('/')[-1],))
#     conn.commit()
#     cur.close()
#     conn.close()
#     return tracked_urls
# experiment()

def table_extract(id):
    html_list=table_counter(id)
    td_strings = []
    for html in html_list:
        td_values = html.split('<td>')
        if len(td_values) > 6:
            td_string = td_values[6].split('</td>')[0].strip()
            td_strings.append(td_string)
        else:
            td_strings.append(None)
    return td_strings

# print(table_counter(216629458))
# print(table_extract(216629458))
# print(len(table_extract(911343667)))
#Table heden huvitai bugluj baigaa esehiig shalgadag function 
def progress(id):
    description_list=table_extract(id)
    count = 0
    for element in description_list:
        if element is not None and element != "":
            count += 1 
    z=count/len(description_list)*100
    return z
# print(list(map(progress,get_id())))

def isdocumented(id):
    if progress(id)>=70 and count_word(id)>=5:
        return("Y")
    else:
        return("N")
    
# print(isdocumented(911343667))
# print(list(map(isdocumented,get_id())))


def send_email_to_unfilled_page_owners():
        conn = None
        try:
            conn = cx_Oracle.connect(
                config.username,
                config.password,
                config.dsn,
                encoding=config.encoding)            
            cur = conn.cursor()  
            sql_engineers="""SELECT DISTINCT(CURRENT_RESPONSIBLE) FROM uni_dynamic_report.Confluence_enforce_table WHERE CURRENT_RESPONSIBLE IN ('esen.d', 'nyamdavaa.yo', 'ariunaa.b', 'baljinnyam.ba', 'chinbayar.a')"""
            cur.execute(sql_engineers)
            sql_engineer_list= cur.fetchall()
            engineer_list = [text[0] for text in sql_engineer_list]
            for engineer in engineer_list:
                try:
                        cur = conn.cursor()  
                        query_engineer_table = f"""SELECT *
                        FROM (
                        SELECT z1.OWNER, z1.table_name, z2.METADATA_LINK, z2.Page_Id, 'new' aa
                        FROM uni_grant_user.t_db_tables_w_detail z1
                        INNER JOIN uni_dynamic_report.Confluence_enforce_table z2
                        ON z1.metadata_link = z2.metadata_link 
                        WHERE (z2.IS_DOCUMENTED IS NULL OR z2.IS_DOCUMENTED = 'N')
                            AND (z2.EMAIL_SENT IS NULL OR z2.EMAIL_SENT ='N') 
                            AND z2.current_responsible = '{engineer}'
                        ORDER BY z1.size_gb 
                        ) WHERE ROWNUM <= 5
                        UNION ALL
                        SELECT z1.OWNER, z1.table_name, z2.METADATA_LINK, z2.Page_Id, 'Transfer'
                        FROM uni_grant_user.t_db_tables_w_detail z1
                        INNER JOIN uni_dynamic_report.Confluence_enforce_table z2
                        ON z1.metadata_link = z2.metadata_link 
                        WHERE (z2.IS_DOCUMENTED IS NULL OR z2.IS_DOCUMENTED = 'N')
                        AND z2.EMAIL_SENT = 'Y'
                        AND z2.NEW_EXECUTOR = '{engineer}'
                        UNION ALL
                        SELECT z1.OWNER, z1.table_name, z2.METADATA_LINK, z2.Page_Id, 'Old'
                        FROM uni_grant_user.t_db_tables_w_detail z1
                        INNER JOIN uni_dynamic_report.Confluence_enforce_table z2
                        ON z1.metadata_link = z2.metadata_link 
                        WHERE (z2.IS_DOCUMENTED IS NULL OR z2.IS_DOCUMENTED = 'N')
                        AND z2.EMAIL_SENT = 'Y'
                        AND z2.current_responsible = '{engineer}'
                        AND z2.METADATA_LINK NOT IN (
                            SELECT z2.METADATA_LINK
                            FROM uni_grant_user.t_db_tables_w_detail z1
                            INNER JOIN uni_dynamic_report.Confluence_enforce_table z2
                            ON z1.metadata_link = z2.metadata_link 
                            WHERE (z2.IS_DOCUMENTED IS NULL OR z2.IS_DOCUMENTED = 'N')
                            AND z2.EMAIL_SENT = 'Y'
                            AND z2.NEW_EXECUTOR = '{engineer}')"""
                        cur.execute(query_engineer_table)  
                        engineer_email_owner_table_status_list = cur.fetchall()
                        if len(engineer_email_owner_table_status_list)>0:
                            list_owner=[]
                            list_table=[]
                            list_link=[]
                            list_id=[]
                            for x in engineer_email_owner_table_status_list:
                                list_owner.append(x[0])
                                list_table.append(x[1])
                                list_link.append(x[2])
                                list_id.append(x[3])
                            list_true_table=[list_owner[i]+"."+list_table[i] for i in range(len(list_owner))]
                            sql = """<table>
                                        <tr>
                                        <th>№</th>
                                        <th>TITLE</th>
                                        <th>LINK</th>"""
                            html = """<!DOCTYPE html>
                                <html>
                                    <head>
                                        <style>
                                        table {
                                            border-collapse: collapse;
                                            width: 100%;
                                            padding: 2px;
                                        }

                                        th, td {
                                            border:1px solid black;
                                            text-align: left;
                                            padding: 2px;
                                        }
                                        th {
                                            background-color: #3fb68e;
                                            color: white;
                                            padding: 3px;
                                        }

                                        </style>
                                    </head>
                                    <body>

                                    """
                            # Remember to unmark title and link

                            num_row=0
                            for table_name,link in zip(list_true_table,list_link):        
                                    num_row +=1         
                                    sql = sql + """<tr>
                                                <td>""" + str(num_row) + """</td>
                                                <td>""" + table_name + """</td>
                                                <td><a href='""" + link + """' target='_blank'>""" + str(link) + """</a></td>
                                            </tr>
                                        """
                            try:
                                email_last = html +  """<b>CONFLUENCE METADATA PAGE FILL REQ</b> <br><br>Сайн байна уу""" + """, CONFLUENCE-н table metadata page-д мэдээлэл оруулах хүсэлт явуулж байна. Та дараах хуудсуудад мэдээлэлээ оруулна уу. <br><br>""" + """<br><br>Please visit <a href="https://unitelgroup.atlassian.net/wiki/spaces/MM/pages/1010532361/Metadata-Page">https://unitelgroup.atlassian.net/wiki/spaces/MM/pages/1010532361/Metadata-Page</a> for more information.<br><br>"""+ sql + """</table><br> <br> BR,<br> Data team </body></html>"""
                                cur.callproc("UNI_DYNAMIC_REPORT.SEND_MAIL_HTML_CLOB", [str(engineer)+"@unitel.mn", "it_data@unitel.mn", "", "CONFLUENCE METADATA PAGE FILL REQ_TEST_EMAIL_APPROVAL", email_last, "10.21.64.80", "26"]) 
                                conn.commit() 
                                for id in list_id:
                                        cur = conn.cursor()  
                                        query_email_sent = f"UPDATE uni_dynamic_report.Confluence_enforce_table SET EMAIL_SENT = 'Y' WHERE PAGE_ID = {id}"
                                        cur.execute(query_email_sent)
                                        conn.commit()                            
                                logging.info("%s: Email sent successfully at %s", engineer, datetime.datetime.now())
                            except Exception as err:
                                logging.error("Error processing table for %s at %s: %s", engineer, datetime.datetime.now(), err)
                        else:
                            logging.error("%s does not have any tables left to process at %s", engineer, datetime.datetime.now())
                except Exception as err:
                        logging.error("Error grabbing engineer or processing table: %s",datetime.datetime.now(), err)
                    
        except cx_Oracle.Error as error:
            logging.error("Database connection error at %s: %s", datetime.datetime.now(), error)
        finally:
            if conn:
                conn.close()



    # mark_documented()

    # send_email_to_unfilled_page_owners()
def main():
    # update_date()
    # update_engineer()
    connection = cx_Oracle.connect(
                config.username,
                config.password,
                config.dsn,
                encoding=config.encoding)
    cursor = connection.cursor()
    process_id_list=get_id()
    for id in process_id_list:
        try:
            connection = cx_Oracle.connect(
                config.username,
                config.password,
                config.dsn,
                encoding=config.encoding)
            cursor = connection.cursor()
            # Your logic to process each id here
            result = isdocumented(id)
            # Push result back to Oracle table
            query_1 = f"UPDATE uni_dynamic_report.Confluence_enforce_table SET IS_DOCUMENTED = '{result}',WORD_COUNT={count_word(id)},TABLE_PROGRESS={progress(id)} WHERE PAGE_ID = {id}"
            cursor.execute(query_1)            
            if cursor.rowcount == 0:
                logging.warning("No row found for ID %s at %s.", id, datetime.datetime.now())
            else:
                logging.info("Document Query executed successfully for ID %s at %s.", id, datetime.datetime.now())
        except Exception as e:
            # Handle any exceptions that occur
            logging.error("Error processing ID %s: %s at %s", id, e, datetime.datetime.now())
            connection.rollback() # Rollback changes for this ID
        else:
            # Commit changes for this ID
            logging.info("Document Query executed successfully for ID %s at %s", id, datetime.datetime.now())
            connection.commit()
    send_email_to_unfilled_page_owners()

    # cursor.close()
    # connection.close()

if __name__ == '__main__':
    main()

