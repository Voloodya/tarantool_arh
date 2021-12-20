#!/usr/bin/env bash

BRANCH=$BRANCH_NAME
ENDPOINT_URL=$S3_ENDPOINT_URL
DOC_DEST="$S3_UPLOAD_PATH/tdg/${BRANCH_NAME}"

aws s3 cp output/json "$DOC_DEST"/json --endpoint-url="$ENDPOINT_URL" --recursive --include "*" --exclude "*.jpg" --exclude "*.png" --exclude "*.svg"
aws s3 cp output/json/_build_en/json/_images "$DOC_DEST"/images_en --endpoint-url="$ENDPOINT_URL" --recursive
aws s3 cp output/json/_build_ru/json/_images "$DOC_DEST"/images_ru --endpoint-url="$ENDPOINT_URL" --recursive

curl --data '{"update_key":"'"$TDG_UPDATE_KEY"'"}' --header "Content-Type: application/json" --request POST "${TDG_UPDATE_URL}${BRANCH}/"
