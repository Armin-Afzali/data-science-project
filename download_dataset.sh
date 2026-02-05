#!/bin/bash

echo "🔽 Downloading Credit Card Fraud Detection Dataset..."

DATASET_URL="https://github.com/nsethi31/Kaggle-Data-Credit-Card-Fraud-Detection/raw/master/creditcard.csv"
OUTPUT_PATH="./data/creditcard.csv"

mkdir -p ./data

curl -L -o "$OUTPUT_PATH" "$DATASET_URL"

if [ -f "$OUTPUT_PATH" ] && [ -s "$OUTPUT_PATH" ]; then
    echo "✅ Dataset downloaded successfully!"
    echo "📊 File size: $(du -h $OUTPUT_PATH | cut -f1)"
    echo "📍 Location: $OUTPUT_PATH"
else
    echo "❌ Download failed. Please download manually from:"
    echo "   $DATASET_URL"
    exit 1
fi
