#!/bin/bash

# enable the sensors
# echo 1 > /sys/bus/i2c/drivers/INA231/3-0045/enable
# echo 1 > /sys/bus/i2c/drivers/INA231/3-0040/enable
# echo 1 > /sys/bus/i2c/drivers/INA231/3-0041/enable
# echo 1 > /sys/bus/i2c/drivers/INA231/3-0044/enable

# # settle two seconds to the sensors get fully enabled and have the first reading
# sleep 2

# Main infinite loop
# while true; do

# ----------- CPU DATA ----------- #

# Node Configuration for CPU Frequency
# CPU0_FREQ=$((`cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq`/1000))" Mhz"
# CPU1_FREQ=$((`cat /sys/devices/system/cpu/cpu1/cpufreq/scaling_cur_freq`/1000))" Mhz"
# CPU2_FREQ=$((`cat /sys/devices/system/cpu/cpu2/cpufreq/scaling_cur_freq`/1000))" Mhz"
# CPU3_FREQ=$((`cat /sys/devices/system/cpu/cpu3/cpufreq/scaling_cur_freq`/1000))" Mhz"
# CPU4_FREQ=$((`cat /sys/devices/system/cpu/cpu4/cpufreq/scaling_cur_freq`/1000))" Mhz"
# CPU5_FREQ=$((`cat /sys/devices/system/cpu/cpu5/cpufreq/scaling_cur_freq`/1000))" Mhz"
# CPU6_FREQ=$((`cat /sys/devices/system/cpu/cpu6/cpufreq/scaling_cur_freq`/1000))" Mhz"
# CPU7_FREQ=$((`cat /sys/devices/system/cpu/cpu7/cpufreq/scaling_cur_freq`/1000))" Mhz"

# CPU Governor
# CPU_GOVERNOR=`cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor`

# Note Configuration for CPU Core Temperature
# This file is written on the following format:
# CPU0 CPU1 CPU2 CPU3
# TMU_FILE=`cat /sys/devices/10060000.tmu/temp`

# We need to slip those in nice variables...
# CPU0_TEMP=`echo $TMU_FILE | awk '{printf $1}'`"C"
# CPU1_TEMP=`echo $TMU_FILE | awk '{printf $2}'`"C"
# CPU2_TEMP=`echo $TMU_FILE | awk '{printf $3}'`"C"
# CPU3_TEMP=`echo $TMU_FILE | awk '{printf $4}'`"C"

# Now lets get CPU Power Comsumption
# Letter Values are:
# V = Volts
# A = Amps
# W = Watts

# A7 Nodes
A7_V=`cat /sys/bus/i2c/drivers/INA231/3-0045/sensor_V`"V"
A7_A=`cat /sys/bus/i2c/drivers/INA231/3-0045/sensor_A`"A"
A7_W=`cat /sys/bus/i2c/drivers/INA231/3-0045/sensor_W`"W"

# A15 Nodes
A15_V=`cat /sys/bus/i2c/drivers/INA231/3-0040/sensor_V`"V"
A15_A=`cat /sys/bus/i2c/drivers/INA231/3-0040/sensor_A`"A"
A15_W=`cat /sys/bus/i2c/drivers/INA231/3-0040/sensor_W`"W"


# --------- MEMORY DATA ----------- # 
# MEM_V=`cat /sys/bus/i2c/drivers/INA231/3-0041/sensor_V`"V"
# MEM_A=`cat /sys/bus/i2c/drivers/INA231/3-0041/sensor_A`"A"
# MEM_W=`cat /sys/bus/i2c/drivers/INA231/3-0041/sensor_W`"W"

# ---------- GPU DATA ------------- # 
# GPU_V=`cat /sys/bus/i2c/drivers/INA231/3-0044/sensor_V`"V"
# GPU_A=`cat /sys/bus/i2c/drivers/INA231/3-0044/sensor_A`"A"
# GPU_W=`cat /sys/bus/i2c/drivers/INA231/3-0044/sensor_W`"W"
# GPU_FREQ=`cat /sys/devices/11800000.mali/clock`" Mhz"

# ---------- FAN Speed ------------- # 
# FAN_SPEED=$((`cat /sys/bus/platform/devices/odroid_fan.14/pwm_duty` * 100 / 255))"%"

# ---------- DRAW Screen ----------- #

# echo "CPU0: $CPU0_FREQ"
# echo "CPU1: $CPU1_FREQ"
# echo "CPU2: $CPU2_FREQ"
# echo "CPU3: $CPU3_FREQ"
# echo "CPU4: $CPU4_FREQ"
# echo "CPU5: $CPU5_FREQ"
# echo "CPU6: $CPU6_FREQ"
# echo "CPU7: $CPU7_FREQ"
# echo "Governor: $CPU_GOVERNOR"
# echo "Fan Speed: $FAN_SPEED"
echo "A15 Power: $A15_V, $A15_A, $A15_W"
echo "A7 Power: $A7_V, $A7_A, $A7_W"
# echo "MEM Power: $MEM_V, $MEM_A, $MEM_W"
# echo "GPU Power: $GPU_V, $GPU_A, $GPU_W @ $GPU_FREQ"

# sleep 3
# clear
# done
