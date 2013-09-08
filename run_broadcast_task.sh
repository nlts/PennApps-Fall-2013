#!/bin/bash

celery -A broadcast_task worker >> broadcast_task.log 2>&1 &
