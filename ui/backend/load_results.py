import json
import os

import requests
from dotenv import load_dotenv

ui_root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
env_local_path = os.path.join(ui_root_dir, ".env.local")
env_path = os.path.join(ui_root_dir, ".env")
if os.path.exists(env_local_path):
    load_dotenv(env_local_path)
else:
    load_dotenv(env_path)
host = os.getenv("VITE_BACKEND_HOST", "127.0.0.1")
port = os.getenv("VITE_BACKEND_PORT", 5000)
base_path = os.getenv("VITE_FRONTEND_BASE_PATH", "")
url = f"http://{host}:{port}{base_path}/api/post"

project_root_dir = os.path.abspath(os.path.join(ui_root_dir, ".."))
assets_dir = os.path.abspath(os.path.join(project_root_dir, "assets"))
type_dirs = os.listdir(assets_dir)
results = []
# Iterate over assets/ and get results automatically
for type_dir in type_dirs:
    type_path = os.path.join(assets_dir, type_dir)
    target_dirs = os.listdir(type_path)
    for target_dir in target_dirs:
        if "template" == target_dir:
            continue
        target_path = os.path.join(type_path, target_dir)
        results_of_target = json.load(open(os.path.join(target_path, "results.json"), "r"))
        for metric in range(1, 3):
            results.append(
                (
                    (
                        results_of_target["target"],
                        results_of_target["targetDisplayedName"],
                        results_of_target["displayedOrder"],
                        results_of_target["isEnable"],
                        results_of_target["type"],
                        metric,
                        results_of_target["ingestTime"],
                        results_of_target["compressedSize"],
                        results_of_target["avgIngestMem"],
                        results_of_target["metrics"][metric - 1]["avgQueryMem"],
                    ),
                    tuple(results_of_target["metrics"][metric - 1]["queryTimes"]),
                )
            )


def dump_and_post():
    """This function construct the request for each benchmark result and send it to the Flask
    backend"""
    headers = {"Content-Type": "application/json"}
    for result in results:
        payload = json.dumps(
            {
                "target": result[0][0],
                "target_displayed_name": result[0][1],
                "displayed_order": result[0][2],
                "is_enable": result[0][3],
                "type": result[0][4],
                "metric": result[0][5],
                "ingest_time": result[0][6],
                "compressed_size": result[0][7],
                "avg_ingest_mem": result[0][8],
                "avg_query_mem": result[0][9],
                "query_times": str(list(result[1])),
            }
        )
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)


dump_and_post()
