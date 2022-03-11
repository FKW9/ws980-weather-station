# ELV WS980 Weather Station Logger

### ELV WS980 Weather Station Logger with Graphite and Grafana on an Synology NAS.
This script is executed every minute on my NAS via the task scheduler.
Requests data from the weather station and sends it to graphite, a time-series database.
This data is then visualized with Grafana.

## Requirements
- [WS980 Weather Station](https://de.elv.com/elv-wifi-wetterstation-ws980wifi-inkl-funk-aussensensor-868-mhz-app-pc-auswertesoftware-250408), or similar
- WiFi connection with known IP of the station
- Python3 and Graphite running on the Host

## Used Hardware
- [WS980 Weather Station](https://de.elv.com/elv-wifi-wetterstation-ws980wifi-inkl-funk-aussensensor-868-mhz-app-pc-auswertesoftware-250408)
- Synology DS218+
