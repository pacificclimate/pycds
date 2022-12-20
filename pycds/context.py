import os


def get_schema_name():
    return os.environ.get("PYCDS_SCHEMA_NAME", "crmp")


def get_cxhx_schema_name():
    return os.environ.get("PYCDS_CXHX_SCHEMA_NAME", f"{get_schema_name()}_cxhx")


def get_su_role_name():
    return os.environ.get("PYCDS_SU_ROLE_NAME", "pcicdba")
