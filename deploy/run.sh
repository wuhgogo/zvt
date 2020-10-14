#!/bin/bash -e

export PYTHONPATH=$PYTHONPATH:/app:/app/examples/recorders

nohup python /app/examples/recorders/eastmoney_data_runner1.py >> /app/logs/eastmoney_data_runner1  2>&1 &
nohup python /app/examples/recorders/eastmoney_data_runner2.py >> /app/logs/eastmoney_data_runner2  2>&1 &
nohup python /app/examples/recorders/joinquant_data_runner1.py >> /app/logs/joinquant_data_runner1  2>&1 &
nohup python /app/examples/recorders/joinquant_data_runner2.py >> /app/logs/joinquant_data_runner2  2>&1 &
nohup python /app/examples/recorders/sina_data_runner.py >> /app/logs/sina_data_runner  2>&1 &