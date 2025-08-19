# Social Media and Marketing Engagement Metrics Pipeline

## Project Overview
This data engineering project focused on developing a data pipeline to extract, transform, and analyse engagement data from major social media platforms, enabling real-time tracking of key metrics and powering marketing contract payments. The goal was to facilitate insightful campaign performance analysis while demonstrating how multiple cloud services can be integrated into a unified analytical framework. After reviewing existing tools and services, the solution was implemented using a hybrid cloud orchestration layer built on AWS and GCP, incorporating AI agents and secure data sources. This serves both as a practical analytics tool and a proof-of-concept for scalable, intelligent, and secure data integration.

## Architecture
- **AWS Lambda Functions:** Serverless backend processing
- **Database:** BigQuery
- **Frontend:** Looker Studio
- **Infrastructure:** AWS-based cloud architecture

## Components

### AWS Lambda Functions (`/aws-lambda/`)
- **telegram-bot-stack-TelegramBotFunction:** Communicates with influencers via Telegram to retrieve Social Media metrics
- **contract-payment-processor:** Evaluates contracts that have reached the metrics in order to release payments
- **Contract_to_JSON:** Transform PDF Contracts into JSON Files to mantain contracts DDBB

## Technologies Used
- airSlate Workflows
- AWS Lambda Functions
- Social Media API Key's
- OpenAI API
- Telegram bot's
- Google BigQuery
- Google WebApp Scripts
- Google Looker Studio
- NLP

## Dissertation Information
- **University:** University College London
- **Degree:** MSc in Business Analytics
- **Year:** 2025
- **Supervisor:** Niall Roche
