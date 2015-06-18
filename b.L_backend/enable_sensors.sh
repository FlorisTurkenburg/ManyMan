#!/bin/bash

# enable the sensors
echo 1 > /sys/bus/i2c/drivers/INA231/3-0045/enable
echo 1 > /sys/bus/i2c/drivers/INA231/3-0040/enable
echo 1 > /sys/bus/i2c/drivers/INA231/3-0041/enable
echo 1 > /sys/bus/i2c/drivers/INA231/3-0044/enable

# settle two seconds to the sensors get fully enabled and have the first reading
sleep 2