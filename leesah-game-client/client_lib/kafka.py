from pathlib import Path
import yaml
from yaml.loader import SafeLoader


def base_config(bootstrap_servers: str) -> dict[str, str]:
    config = {"bootstrap.servers": bootstrap_servers}
    if "localhost" not in bootstrap_servers:
        # CA_PATH = Path("certs/ca.pem")
        # creds = json.loads(Path("certs/leesah_creds.json").open(mode="r").read())
        with open(Path("certs/student-certs.yaml"), encoding="utf-8") as file:
            creds = yaml.load(
                file.read(),
                Loader=SafeLoader,
            )

        config.update(
            {
                "security.protocol": "SSL",
                "ssl.ca.pem": creds["ca"],
                "ssl.key.pem": creds["user"]["access_key"],
                "ssl.certificate.pem": creds["user"]["access_cert"],
            }
        )
    return config


def consumer_config(
    bootstrap_servers: str, group_id: str, auto_commit: bool
) -> dict[str, str]:
    return base_config(bootstrap_servers) | {
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": str(auto_commit),
    }


def producer_config(bootstrap_servers: str) -> dict[str, str]:
    return base_config(bootstrap_servers) | {"broker.address.family": "v4"}
