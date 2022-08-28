#!/bin/bash

# influxdb installation

wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.4.0-amd64.deb
sudo dpkg -i influxdb2-2.4.0-amd64.deb

sudo apt update
sudo apt upgrade -y
 