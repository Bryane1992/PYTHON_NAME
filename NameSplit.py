import re

import pypyodbc


def main():
    name_list = get_name_list()

    for name_dict in name_list:
        print("".join(["working on:", name_dict["phx_name_full"]]))

        phx_last_name = re.search("([^,]+)", name_dict["phx_name_full"])

        name_dict["phx_name_last"] = phx_last_name.group(1)

        '''

        the last name updates correctly, but we forgot to update the first and middle names!

        your code here





        '''

        update_name(name_dict)


def update_name(name_dict):
    theargs = [name_dict["phx_name_first"], name_dict["phx_name_mid"], name_dict["phx_name_last"],
               name_dict["phx_name_id"]]

    thesql = """UPDATE [dbo].[PHX_NAME] SET

    [PHX_NAME_FIRST] = ?,

    [PHX_NAME_MID] = ?,

    [PHX_NAME_LAST] = ?

    WHERE [PHX_NAME_ID] = ?

    """

    connection = get_connection()

    cursor = connection.cursor()

    cursor.execute(thesql, theargs)

    connection.commit()

    cursor.close()

    connection.close()


def get_name_list():
    thesql = """

    SELECT [PHX_NAME_ID] AS phx_name_id

    ,[PHX_NAME_FULL] AS phx_name_full

    ,[PHX_NAME_FIRST] AS phx_name_first

    ,[PHX_NAME_MID] AS phx_name_mid

    ,[PHX_NAME_LAST] phx_name_last

    FROM [dbo].[PHX_NAME]

    """

    name_list = []

    connection = get_connection()

    cursor = connection.cursor()

    cursor.execute(thesql)

    for row in cursor:
        name_dict = {

            "phx_name_id": row["phx_name_id"],

            "phx_name_full": row["phx_name_full"],

            "phx_name_first": row["phx_name_first"],

            "phx_name_mid": row["phx_name_mid"],

            "phx_name_last": row["phx_name_last"],

        }

        name_list.append(name_dict)

    cursor.close()

    connection.close()

    return name_list


def get_connection():
    server_name = "localhost\SQLEXPRESS01"

    database = "test"

    '''setup connection depending on which db we are going to write to in which environment'''

    connection = pypyodbc.connect(

        "Driver={SQL Server};"

        "Server=" + server_name + ";"

                                  "Database=" + database + ";"

                                                           "Trusted_Connection=yes;"

        # "Trusted_Connection=no;UID=" + username + ";PWD=" + password + ";"

    )

    return connection


main()