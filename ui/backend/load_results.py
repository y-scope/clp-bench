import json
import os

import requests
from dotenv import load_dotenv

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
env_local_path = os.path.join(root_dir, ".env.local")
env_path = os.path.join(root_dir, ".env")
if os.path.exists(env_local_path):
    load_dotenv(env_local_path)
else:
    load_dotenv(env_path)
host = os.getenv("VITE_BACKEND_HOST", "127.0.0.1")
port = os.getenv("VITE_BACKEND_PORT", 5000)
base_path = os.getenv("VITE_FRONTEND_BASE_PATH", "")
url = f"http://{host}:{port}{base_path}/api/post"
print(url)

# All current results
results = [
    # Unstructured with hot run
    (
        ("clpg", "CLP & CLG", 1, False, 1, 1, 2871750, 6406600131, 21300206633, 971736351),
        (23670, 105240, 16200, 16630, 19000, 197160, 201620, 19260, 22590, 8160, 5660, 25080, 1840),
    ),
    (
        ("glt", "CLP", 2, True, 1, 1, 3161250, 3758274642, 1486115308, 1712198779),
        (2620, 2400, 2690, 3280, 2260, 55270, 54100, 2330, 4590, 5250, 4610, 4150, 1870),
    ),
    (
        (
            "elasticsearch",
            "Elasticsearch",
            3,
            True,
            1,
            1,
            117772160,
            124610255258,
            54708970455,
            14068779008,
        ),
        (660, 550, 27860, 42030, 480, 1010, 100910, 570, 66480, 520, 560, 8530, 450),
    ),
    (
        ("loki", "Loki", 4, True, 1, 1, 8661810, 26788642161, 43078521979, 25583723479),
        (
            651500,
            764890,
            1546260,
            4950080,
            517290,
            509460,
            2790890,
            576470,
            3362330,
            450830,
            435630,
            940180,
            467020,
        ),
    ),
    (
        ("splunk", "Splunk", 5, True, 1, 1, 33861600, 94725503713, 648871831, 935190157),
        (
            280,
            1260,
            58760,
            61650,
            310,
            8560,
            647110,
            11660,
            35060,
            260,
            340,
            14100,
            30,
        ),
    ),
    (
        ("grep", "grep", 999, True, 1, 1, 0, 0, 0, 0),
        (
            494670,
            514690,
            403070,
            373030,
            540660,
            666190,
            604870,
            640610,
            798320,
            443490,
            839100,
            670170,
            394690,
        ),
    ),
    # Unstructured with cold run
    (
        ("clpg", "CLP & CLG", 1, False, 1, 2, 2871750, 6406600131, 21300206633, 5158612992),
        (26581, 112420, 18457, 18647, 20206, 198129, 202447, 20038, 24216, 8901, 6003, 26140, 1986),
    ),
    (
        ("glt", "CLP", 2, True, 1, 2, 3161250, 3758274642, 1486115308, 1149728768),
        (2844, 2576, 2728, 3359, 2434, 55033, 57339, 2715, 4795, 5934, 5364, 4656, 2016),
    ),
    (
        (
            "elasticsearch",
            "Elasticsearch",
            3,
            True,
            1,
            2,
            117772160,
            124610255258,
            54708970455,
            14075236352,
        ),
        (1620, 3170, 37300, 47290, 510, 23820, 107570, 580, 76530, 470, 590, 10310, 490),
    ),
    (
        ("loki", "Loki", 4, True, 1, 2, 8661810, 26788642161, 43078521979, 26403311452),
        (
            658560,
            639740,
            1403050,
            4655970,
            389030,
            364110,
            2608620,
            412570,
            3404270,
            108990,
            108410,
            825040,
            367430,
        ),
    ),
    (
        ("splunk", "Splunk", 5, True, 1, 2, 33861600, 94725503713, 648871831, 985824960),
        (
            3640,
            37400,
            175810,
            70110,
            4040,
            61870,
            1213550,
            21750,
            48020,
            2800,
            430,
            22020,
            50,
        ),
    ),
    (
        ("grep", "grep", 999, True, 1, 2, 0, 0, 0, 0),
        (
            494670,
            514690,
            403070,
            373030,
            540660,
            666190,
            604870,
            640610,
            798320,
            443490,
            839100,
            670170,
            394690,
        ),
    ),
    # Semi-structured with hot run
    (
        ("clps", "CLP-S", 1, True, 2, 1, 786770, 381346120, 466773606, 135224361),
        (1260, 28440, 1180, 1730, 1410, 1190),
    ),
    (
        ("clpJson", "CLP-JSON", 2, False, 2, 1, 1214800, 380842803, 1731786179, 1683677512),
        (6150, 12770, 5600, 5440, 5110, 5060),
    ),
    (
        (
            "elasticsearch",
            "Elasticsearch",
            3,
            True,
            2,
            1,
            19657020,
            19141618565,
            10376729068,
            5961930752,
        ),
        (2280, 9440, 490, 590, 2110, 1700),
    ),
    # Semi-structured with cold run
    (
        ("clps", "CLP-S", 1, True, 2, 2, 786770, 381346120, 466773606, 164731290),
        (1300, 28560, 1170, 1700, 1420, 1210),
    ),
    (
        ("clpJson", "CLP-JSON", 2, False, 2, 2, 1214800, 380842803, 1731786179, 1500491285),
        (18360, 15690, 5060, 5640, 5030, 5340),
    ),
    (
        (
            "elasticsearch",
            "Elasticsearch",
            3,
            True,
            2,
            2,
            19657020,
            19141618565,
            10376729068,
            6565423104,
        ),
        (3050, 9270, 520, 560, 6000, 1660),
    ),
]


def dump_and_post():
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
