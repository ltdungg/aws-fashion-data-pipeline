# AWS Fashion Data Pipeline

## Overview

AWS Fashion Data Pipeline is a comprehensive data engineering project designed to build a data lake on AWS for an online clothing store. This project integrates data from multiple sources, including Amazon RDS (Relational Database Service) and Amazon Kinesis, to create a robust analytics pipeline for fashion product recommendations.

## Project Architecture

The pipeline follows a modern data lake architecture with several key components:

- **Data Ingestion Layer**: Collects data from RDS databases and real-time streams via Kinesis
- **Storage Layer**: Organizes data in Amazon S3 with different zones (landing, raw, curated)
- **Processing Layer**: Transforms data using AWS Glue and Lambda functions
- **Cataloging Layer**: Manages metadata using AWS Glue Data Catalog
- **Consumption Layer**: Enables analytics and recommendation generation
- **Security & Governance Layer**: Implements data protection and access controls

## Key Features

- Integration of batch data from RDS and streaming data from Kinesis
- Serverless architecture using AWS managed services
- ETL processes for data transformation and enrichment
- Product recommendation system based on customer behavior analysis
- Scalable infrastructure that can handle growing data volumes

## Project Structure

The repository is organized into the following directories:

- **ec2/**: Contains scripts and configurations for EC2 instances
- **glue/**: Includes AWS Glue ETL jobs and workflows
- **lambda/**: Houses Lambda functions for real-time data processing

## Technologies Used

- **Amazon RDS**: For storing structured data like product information and customer profiles
- **Amazon Kinesis**: For capturing real-time user interactions and clickstream data
- **AWS Glue**: For ETL processes and data catalog management
- **AWS Lambda**: For serverless computing and real-time data processing
- **Amazon S3**: For data lake storage in various formats

## Getting Started

To get started with this project, follow these steps:

1. Clone this repository to your local machine
2. Set up the required AWS services (RDS, Kinesis, Glue, Lambda, S3)
3. Configure your AWS credentials for access to these services
4. Follow the detailed workshop instructions available at the documentation site

## Detailed Documentation

For comprehensive step-by-step instructions, architecture details, and hands-on exercises, please visit the project documentation site:

👉 [AWS Fashion Pipeline Documentation](https://ltdungg.github.io/aws-fashion-data-pipeline-doc/)

## Benefits

- **Real-time Analytics**: Process and analyze customer behavior as it happens
- **Personalized Recommendations**: Deliver tailored product suggestions to customers
- **Scalable Architecture**: Handle growing data volumes without performance degradation
- **Cost Optimization**: Utilize serverless components to reduce operational costs
- **Data Governance**: Implement proper security and access controls for sensitive data

## Future Enhancements

- Integration with advanced machine learning models for better predictions
- Support for multi-modal data including image and text analysis for fashion products
- Enhanced monitoring and alerting capabilities
- Multi-region deployment for global markets

## License

This project is available for public use and modification.
