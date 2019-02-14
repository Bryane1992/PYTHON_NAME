import re

import pypyodbc


def main():
    name_list = get_name_list()

    for name_dict in name_list:
        print("".join(["working on:", name_dict["phx_name_full"]]))

        # Match a single character in the list
        # + matches between 1 - unlimited times
        # in the range A-Za-z and allows -, ' that some names have like Paul D'Angelo
        phx_split_name = re.findall("[A-Za-z\\-\\']+", name_dict["phx_name_full"])

        # If there is only two words then only update the first and last name and set mid to none
        if len(phx_split_name) == 2:
            # if theres only two words then theres no middle name so do a positive look behind
            # starting at '' excluding whitespace and comma and get group 1
            name_dict["phx_name_first"] = str(re.search("([^\\s]+(?<=)$)", name_dict["phx_name_full"]).group(1))
            # set middle name to none as theres no middle name
            name_dict["phx_name_mid"] = None
            # Get group 1 excluding the comma and since last name is first it gets last name
            name_dict["phx_name_last"] = str(re.search("([^,]+)", name_dict["phx_name_full"]).group(1))

        elif len(phx_split_name) == 3:
            # For occasions where there is a . in the first name we need to seperate just the first and middle
            first_middle = re.search("[^,]+$", name_dict["phx_name_full"]).group()
            # now regex look ahead until we find a . or a whitespace
            first = re.search("([^,]+(?=\\.)|[^,]+(?= ))", first_middle).group()
            name_dict["phx_name_first"] = str(first)
            name_dict["phx_name_mid"] = str(re.search("([^,\\s]+(?<=)$)", name_dict["phx_name_full"]).group(1))
            name_dict["phx_name_last"] = str(re.search("(([^,]+))", name_dict["phx_name_full"]).group(1))

        else:
            raise ValueError('This is bad format and needs to be in the proper format, Last, First Middle, Last, First')

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