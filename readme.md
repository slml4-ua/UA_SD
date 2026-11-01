# WaterManagement – Distributed Systems, Practical Assignment 1 (2026/2027)

Distributed system for managing irrigation stations. Author: Sergio Llopis (`slml4`).

## Folders
- `WM_Central/` – central module (SQLite, Kafka, socket server, status panel)
- `WM_FO/` – field operator application
- `WM_WS/WM_WS_M/` – station monitor (sockets)
- `WM_WS/WM_WS_E/` – station engine (valve and flow meter simulation)
- `prompts/` – prompts used with AI tools

## Cloud (AWS, `32.198.203.218`, Kafka 9092, Central 5050)
```
sudo docker ps
sudo docker attach central
```

## Lab (Docker)
```
docker run -it --rm slml4/wm-fo 32.198.203.218 9092 OP1

docker network create wm-net
docker run -it --rm --name wm-monitor --network wm-net slml4/wm-monitor 6060 32.198.203.218 5050 WS1 "Alicante"
docker run -it --rm --network wm-net -v wm-data-ws1:/data slml4/wm-engine 32.198.203.218 9092 wm-monitor 6060
```

Full deployment guide in the report.
