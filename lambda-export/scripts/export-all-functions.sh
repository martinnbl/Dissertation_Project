#!/bin/bash

REGION="eu-west-2"
FUNCTIONS=("telegram-bot-stack-TelegramBotFunction-fqeAhyC56WWc" "contract-payment-processor" "Contract_to_JSON")

echo "🚀 Exporting all Lambda functions from region: $REGION"

for FUNCTION_NAME in "${FUNCTIONS[@]}"; do
    echo ""
    echo "=== Processing: $FUNCTION_NAME ==="
    
    # Create function-specific directory
    mkdir -p "functions/$FUNCTION_NAME"
    mkdir -p "layers/$FUNCTION_NAME"
    
    # Download function code
    echo "📦 Downloading function code..."
    cd "functions/$FUNCTION_NAME"
    aws lambda get-function --function-name "$FUNCTION_NAME" --region $REGION --query 'Code.Location' --output text | xargs curl -o code.zip
    
    # Extract function code
    echo "📂 Extracting function code..."
    unzip code.zip
    rm code.zip
    cd ../..
    
    # Get layers information
    echo "🔍 Finding attached layers..."
    aws lambda get-function-configuration --function-name "$FUNCTION_NAME" --region $REGION --query 'Layers[].[LayerArn]' --output text > temp_layers.txt
    
    # Download each layer for this function
    echo "📥 Downloading layers..."
    cd "layers/$FUNCTION_NAME"
    while read layer_arn; do
        if [ ! -z "$layer_arn" ] && [ "$layer_arn" != "None" ]; then
            layer_name=$(echo $layer_arn | cut -d: -f6)
            version=$(echo $layer_arn | cut -d: -f7)
            
            echo "Downloading layer: $layer_name (version $version)"
            aws lambda get-layer-version --layer-name $layer_name --version-number $version --region $REGION --query 'Content.Location' --output text | xargs curl -o ${layer_name}-v${version}.zip
            
            # Extract layer
            mkdir -p ${layer_name}-v${version}
            cd ${layer_name}-v${version}
            unzip ../${layer_name}-v${version}.zip
            cd ..
        fi
    done < ../../temp_layers.txt
    
    cd ../..
    rm temp_layers.txt
    
    echo "✅ $FUNCTION_NAME exported!"
done

echo ""
echo "🎉 All functions exported successfully!"
echo "Check the functions/ and layers/ directories"