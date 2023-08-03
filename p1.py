#!/usr/bin/env python3
import re
import time
import datetime
from dataclasses import dataclass

import serial

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.write.point import _convert_timestamp


influxdb_token = ""
influxdb_org = ""
influxdb_highres_bucket = "p1_hr"  # With ~24h retention
influxdb_lowres_bucket = "p1_lr"  # With unlimited retention
influxdb_url = "http://localhost:8086"



# Timestamp format used in P1
P1_TIME_FMT = "%y%m%d%H%M%S"

# P1 message should never be more than this number of lines
P1_MSG_LIMIT = 100

# /ISK5\2M550E-1013
regex_p1_header = re.compile(r"^/ISK5.*$")

# !AEB6
regex_p1_crc = re.compile(r"^\!(?P<crc>[A-F0-9]{4})$")

# 0-0:1.0.0(210214195440W)
regex_p1_timestamp = re.compile(r"^0-0:1\.0\.0\((?P<timestamp>\d+)(?P<dst>[WS])\)$")

# 1-0:1.7.0(00.234*kW)
regex_current_kwatt = re.compile(r"^1-0:1\.7\.0\((?P<kwatt>\d+\.\d+)\*kW\)$")

# 1-0:1.8.1(000032.289*kWh)
regex_tariff1_total = re.compile(r"^1-0:1\.8\.1\((?P<kwh>\d+\.\d+)\*kWh\)$")

# 1-0:1.8.2(000016.064*kWh)
regex_tariff2_total = re.compile(r"^1-0:1\.8\.2\((?P<kwh>\d+\.\d+)\*kWh\)$")

# 1-0:32.7.0(235.2*V)
regex_l1_voltage = re.compile(r"^1-0:32\.7\.0\((?P<voltage>\d+\.\d+)\*V\)$")

# 0-1:24.2.1(210214195003W)(00026.800*m3)
regex_gas_total = re.compile(r"^0-1:24\.2\.1\((?P<timestamp>\d+)(?P<dst>[WS])\)\((?P<gas>\d+\.\d+)\*m3\)$")


def p1_localtime_to_utc(timestring, dstchar):
    """Convert P1 timestring in local time to UTC datetime object"""
    # TODO err handling
    local = datetime.datetime.strptime(timestring, P1_TIME_FMT)
    offset = time.timezone if dstchar == "W" else time.altzone
    return local + datetime.timedelta(seconds=offset)


@dataclass
class P1Data:
    timestamp: datetime.datetime = None
    watt: float = None
    tariff1: float = None
    tariff2: float = None
    voltage: float = None
    gas: float = None
    gas_timestamp: datetime = None
    

def p1_data_from_lines(lines):
    p1 = P1Data()

    for line in lines:
        match = regex_p1_timestamp.match(line)
        if match:
            timestring = match.group("timestamp")
            dst = match.group("dst")
            p1.timestamp = p1_localtime_to_utc(timestring, dst)

        match = regex_current_kwatt.match(line)
        if match:
            p1.watt = float(match.group("kwatt")) * 1000  # TODO Err handling

        match = regex_tariff1_total.match(line)
        if match:
            p1.tariff1 = float(match.group("kwh"))

        match = regex_tariff2_total.match(line)
        if match:
            p1.tariff2 = float(match.group("kwh"))

        match = regex_l1_voltage.match(line)
        if match:
            p1.voltage = float(match.group("voltage"))

        match = regex_gas_total.match(line)
        if match:
            timestring = match.group("timestamp")
            dst = match.group("dst")
            p1.gas_timestamp = p1_localtime_to_utc(timestring, dst)
            p1.gas = float(match.group("gas"))

    return p1


def add_p1_to_influxdb(db, p1):
    points = (
        Point("electricity").tag("reading", "power").field("watt", p1.watt).time(p1.timestamp),
        Point("electricity").tag("reading", "tariff1").field("kwh", p1.tariff1).time(p1.timestamp),
        Point("electricity").tag("reading", "tariff2").field("kwh", p1.tariff2).time(p1.timestamp),
        Point("electricity").tag("reading", "voltage").field("volt", p1.voltage).time(p1.timestamp),
        Point("gas").tag("reading", "usage").field("m3", p1.gas).time(p1.gas_timestamp) 
    )

    # Short retention / high-res bucket, store every measurement (1 / sec)
    db.write(influxdb_highres_bucket, influxdb_org, points, WritePrecision.S)

    # For long term / low-res bucket, only store once per minute
    if p1.timestamp.second == 0:
        db.write(influxdb_lowres_bucket, influxdb_org, points, WritePrecision.S)

    db.flush()

    print(f"Written new measurement: {p1}")


if __name__ == "__main__":
    meter = serial.Serial("/dev/ttyUSB0", 115200)
    influxdb = InfluxDBClient(url=influxdb_url, token=influxdb_token)
    writer = influxdb.write_api(write_options=SYNCHRONOUS)

    while True:
        line = meter.readline().decode("utf-8").strip()

        # Read until we see the header
        if regex_p1_header.match(line):
            p1_msg = []

            # Read until trailing CRC (error when it takes too long)
            while len(p1_msg) <= P1_MSG_LIMIT:
                line = meter.readline().decode("utf-8").strip()

                match = regex_p1_crc.match(line)
                if match:
                    crc = match.group("crc")
                    p1 = p1_data_from_lines(p1_msg)
                    add_p1_to_influxdb(writer, p1)
                    break  # Skip else clause
                else:
                    p1_msg.append(line)
            else:
                print("OOOP! Already expected to have seen the CRC message.")

