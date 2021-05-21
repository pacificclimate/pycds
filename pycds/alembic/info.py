from alembic.script import ScriptDirectory
from alembic.config import Config


config = Config()
config.set_main_option("script_location", "pycds:alembic")
script = ScriptDirectory.from_config(config)


def get_current_head():
    return script.get_current_head()


