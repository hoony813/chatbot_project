#!/bin/bash

rm *.zip
zip spotify.zip -r *

aws s3 rm s3://spotify-lambda-hoony813/spotify.zip
aws s3 cp ./spotify.zip s3://spotify-lambda-hoony813/spotify.zip
aws lambda update-function-code --function-name spotify-lambda --s3-bucket spotify-lambda-hoony813 --s3-key spotify.zip
