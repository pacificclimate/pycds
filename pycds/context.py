import os


def get_schema_name():
    return os.environ.get('PYCDS_SCHEMA_NAME', 'crmp')


def get_su_role_name():
    return os.environ.get('PYCDS_SU_ROLE_NAME', 'pcicdba')
