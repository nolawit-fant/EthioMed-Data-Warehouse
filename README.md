# 10 Academy: Artificial Intelligence Mastery Program - Week 7 Challenge

## Project: Building a Data Warehouse for Ethiopian Medical Business Data

### Overview
As a data engineer at **Kara Solutions**, a data science company with over 50+ data-centric solutions, you are tasked with building a data warehouse to store and analyze data on Ethiopian medical businesses, sourced from public Telegram channels and web data. This project involves setting up data scraping, data cleaning and transformation pipelines, object detection using YOLO, and finally, deploying the data for use through a FastAPI application.

This data warehouse will enable efficient querying and reporting, providing comprehensive insights into Ethiopian medical businesses, allowing for better decision-making and trend analysis. 

### Project Objectives
- **Develop a data scraping and collection pipeline** to gather data from specified Telegram channels and websites.
- **Clean and transform data** for consistency and readiness for analysis.
- **Implement object detection** using YOLO to analyze images from Telegram channels.
- **Design and implement a data warehouse** for secure, efficient storage and retrieval of data.
- **Expose the data warehouse via a FastAPI** application to enable easy access to the processed data.

---

## Table of Contents
- [Data and Features](#data-and-features)
- [Competency Mapping](#competency-mapping)
- [Team](#team)
- [Key Dates](#key-dates)
- [Deliverables](#deliverables)
- [Tasks](#tasks)
    - [Task 1: Data Scraping and Collection](#task-1-data-scraping-and-collection)
    - [Task 2: Data Cleaning and Transformation](#task-2-data-cleaning-and-transformation)
    - [Task 3: Object Detection with YOLO](#task-3-object-detection-with-yolo)
    - [Task 4: Exposing Data with FastAPI](#task-4-exposing-data-with-fastapi)

---

## Data and Features
Data is scraped from the following Telegram channels:
- [DoctorsET](https://t.me/DoctorsET)
- [Chemed Telegram Channel](https://t.me/lobelia4cosmetics)
- [Yetena Weg](https://t.me/yetenaweg)
- [EAHCI](https://t.me/EAHCI)

Object detection will be applied to images from the Chemed Telegram Channel and other medical and pharmaceutical channels. Key features to be extracted include business names, product details, prices, and locations.

## Competency Mapping
The project integrates competencies in:
- Data engineering and ETL/ELT processes
- Web scraping and data collection
- Data cleaning and transformation
- Object detection using YOLO
- API development with FastAPI

## Team
This project is led by the Data Engineering team at Kara Solutions.

## Key Dates
- **Start Date**: [Enter start date]
- **Completion Date**: [Enter end date]
- **Submission Date**: [Enter submission date]

## Deliverables
- **Data Scraping Pipeline**: A fully functional pipeline to extract and store data from Telegram channels.
- **Data Warehouse**: A centralized storage solution for cleaned and transformed data.
- **Object Detection Model**: YOLO model implementation to analyze images.
- **API Application**: FastAPI endpoints to access and manage the data warehouse.

---

## Tasks

### Task 1: Data Scraping and Collection
1. **Setup**: Use the Telegram API with Python packages like `telethon` to scrape content from Telegram channels.
2. **Store Raw Data**: Initial storage can be done in temporary files or a local database.
3. **Monitoring and Logging**: Implement logging to monitor scraping progress and capture any errors.

### Task 2: Data Cleaning and Transformation
1. **Data Cleaning**:
    - Remove duplicates
    - Handle missing values
    - Standardize formats
    - Validate data accuracy
2. **Transformation with DBT**:
    - Set up a DBT project to manage data transformations.
    - Use DBT models to define and run SQL transformations, loading data into the data warehouse.
    - Test and document transformations to ensure data quality.

### Task 3: Object Detection with YOLO
1. **Environment Setup**: Install YOLO with dependencies such as OpenCV, PyTorch, or TensorFlow.
2. **Model Deployment**: Use pre-trained YOLO models to detect objects in images from Telegram channels.
3. **Data Processing**: Extract relevant information (bounding boxes, labels, etc.) and store it in the data warehouse.

### Task 4: Exposing Data with FastAPI
1. **Environment Setup**: Install FastAPI and Uvicorn.
2. **Database Configuration**: Configure the database connection in `database.py`.
3. **Data Models and Schemas**: Define SQLAlchemy models in `models.py` and Pydantic schemas in `schemas.py`.
4. **CRUD Operations and Endpoints**: Implement basic CRUD operations and define API endpoints to access the data warehouse.

--- 

This project provides a robust infrastructure to centralize, analyze, and expose critical data on Ethiopian medical businesses, enabling more comprehensive and timely insights.
