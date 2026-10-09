# rtype-relay

WebSocket relay for the co-op and the lobby of the R-Type remake (`python relay.py`, `PORT` from the environment).

Countries of the players come from `geo.bin`, made by `geo_build.py` from the free
[IP Geolocation by DB-IP](https://db-ip.com) "IP to Country Lite" database (CC BY 4.0). Refresh it now and then:
`python geo_build.py dbip-country-lite-YYYY-MM.csv.gz`.
