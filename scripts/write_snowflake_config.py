import os
import pathlib

account  = os.environ["SF_ACCOUNT"]
user     = os.environ["SF_USER"]
password = os.environ["SF_PASSWORD"]

config = (
    "[connections.telcostream]\n"
    f"account  = '{account}'\n"
    f"user     = '{user}'\n"
    f"password = '{password}'\n"
    "role     = 'ACCOUNTADMIN'\n"
)

p = pathlib.Path.home() / ".snowflake" / "config.toml"
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(config)
p.chmod(0o600)
print("config.toml written successfully")