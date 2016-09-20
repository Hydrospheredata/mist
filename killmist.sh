#!/usr/bin/env bash

ps -aef | grep "mist" | awk '{print $2}' | xargs kill -9
