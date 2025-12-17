import os, json, logging
import datetime as dt
import requests

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.eventhub import EventHubProducerClient, EventData

app = func.FunctionApp()

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

def _get_secret(vault_name: str, secret_name: str) -> str:
    credential = DefaultAzureCredential()
    kv_uri = f"https://{vault_name}.vault.azure.net"
    client = SecretClient(vault_url=kv_uri, credential=credential)
    return client.get_secret(secret_name).value

@app.function_name(name="crypto_ingest_timer")
@app.schedule(schedule="0 */1 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def crypto_ingest_timer(mytimer: func.TimerRequest) -> None:
    logging.info("crypto_ingest_timer triggered")

    vault_name = os.environ["KEYVAULT_NAME"]
    secret_name = os.environ["EVENTHUB_SECRET_NAME"]
    eventhub_name = os.environ["EVENTHUB_NAME"]

    eh_conn_str = _get_secret(vault_name, secret_name)

    params = {"ids": "bitcoin,ethereum", "vs_currencies": "usd"}
    r = requests.get(COINGECKO_URL, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    payload = {
        "ts_utc": dt.datetime.utcnow().isoformat(),
        "source": "coingecko",
        "prices": {
            "btc_usd": data.get("bitcoin", {}).get("usd"),
            "eth_usd": data.get("ethereum", {}).get("usd"),
        }
    }

    producer = EventHubProducerClient.from_connection_string(
        conn_str=eh_conn_str,
        eventhub_name=eventhub_name
    )
    with producer:
        producer.send_batch([EventData(json.dumps(payload))])

    logging.info(f"Sent: {payload}")
