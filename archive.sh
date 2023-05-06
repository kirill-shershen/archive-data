#!/bin/bash

rm qa-events-archive-test.zip

zip -r qa-events-archive-test.zip lib/ config.py lambda_function.py db.py exceptions.py cu-ecs-qa.cer

aws lambda update-function-code --function-name  qa-events-archive-test37  --zip-file fileb://qa-events-archive-test.zip
