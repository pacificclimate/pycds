import os
import re


def split_on(sep):
    def f(s):
        if s == "":
            return []
        return re.split(fr"\s*{sep}\s*", s)
    return f


def parse_standard_table_privileges(stp):
    """A standard table privilege spec is a string of the form
        role: priv, priv, priv, ...; role: priv, priv, priv, ...; ...
    The named role is granted the privileges listed after the colon following it.
    Privileges are separated by commas. A sequence of such role-priv items is separated
    by semicolons.
    """
    split_items = split_on(';')
    split_item = split_on(':')

    def split_privs(rp):
        return rp[0], split_on(',')(rp[1])

    return list(map(split_privs, map(split_item, split_items(stp))))


def get_standard_table_privileges(
    default_privs="inspector: SELECT; viewer: SELECT; steward: ALL"
):
    """Get and parse an environment variable defining the standard privileges to be
    applied to new table-like) objects (tables, views, matviews.
    """
    env = os.environ.get("PYCDS_STANDARD_TABLE_PRIVS", default_privs)
    return parse_standard_table_privileges(env)



def get_schema_name():
    return os.environ.get("PYCDS_SCHEMA_NAME", "crmp")


def get_su_role_name():
    return os.environ.get("PYCDS_SU_ROLE_NAME", "pcicdba")
