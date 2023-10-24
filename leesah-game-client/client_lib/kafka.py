from pathlib import Path
from typing import Dict
import yaml
from yaml.loader import SafeLoader


def base_config(bootstrap_servers: str) -> Dict:
    config = {"bootstrap.servers": bootstrap_servers}
    if "localhost" not in bootstrap_servers:
        # CA_PATH = Path("certs/ca.pem")
        # creds = json.loads(Path("certs/leesah_creds.json").open(mode="r").read())
        creds = yaml.load(
            Path("certs/student-certs.yaml")
            .open(mode="r", encoding="utf-8")
            .read(),
            Loader=SafeLoader,
        )
        config |= {
            "security.protocol": "SSL",
            "ssl.ca.pem": creds["ca"],
            "ssl.key.pem": creds["user"]["access_key"],
            "ssl.certificate.pem": creds["user"]["access_cert"],
        }
    return config


def consumer_config(bootstrap_servers: str, group_id: str, auto_commit: bool):
    config = base_config(bootstrap_servers)
    return config | {
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": str(auto_commit),
    }


def producer_config(bootstrap_servers: str):
    return base_config(bootstrap_servers) | {"broker.address.family": "v4"}
