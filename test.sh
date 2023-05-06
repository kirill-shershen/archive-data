#!/bin/bash

ENV=test py.test tests.py -p no:sugar --disable-warnings
