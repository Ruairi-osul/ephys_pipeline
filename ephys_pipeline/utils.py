import json
import dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .dal import DAL_ORM
import os


def read_json(json_path):
    with open(json_path) as f:
        out = json.load(f)
    return out


def get_connection_string(environment_dict, no_db=False):
    connection_string = (
        f"{environment_dict.get('DBMS')}+{environment_dict.get('DB_DRIVER')}://"
        f"{environment_dict.get('DB_USER')}:{environment_dict.get('DB_PASSWORD')}"
        f"@{environment_dict.get('DB_HOST')}:{environment_dict.get('DB_PORT')}"
    )
    if not no_db:
        connection_string = "/".join(
            [connection_string, environment_dict.get("DB_NAME")]
        )
    return connection_string


def _prep_db(obj):
    dotenv.load_dotenv()
    setattr(obj, "engine", create_engine(get_connection_string(os.environ)))
    setattr(obj, "Session", sessionmaker(bind=obj.engine))
    setattr(obj, "orm", DAL_ORM(engine=obj.engine))


def make_filename(*args, ext: str, sep="_"):
    """
    Returns a filename
    params:
        - *args: all subcomponents of the filename
        - ext: the extention MUST INCLUDE THE '.'
        - sep: the separator for the filename subcomponents
    """
    return sep.join(list(args)) + ext
