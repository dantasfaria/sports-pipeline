# 🏆 Football Data Pipeline Project
This project is a comprehensive data pipeline designed to fetch, process, and transform football data from "Brasileirão", the most important brazilian championship of football. The pipeline utilizes Apache Airflow for workflow management, Apache Spark for data processing, and Amazon S3 for data storage. The project's primary goal is to provide a scalable and efficient data pipeline that can handle large volumes of football data, making it easier to analyze and gain insights from the data.

## 🚀 Features
* Fetches data from a football API and writes it to a bronze layer
* Transforms raw data from the bronze layer to a silver layer using Apache Spark
* Processes data from the silver layer to a feature layer using Apache Spark
* Utilizes Apache Airflow for workflow management and scheduling
* Stores data in Amazon S3 for scalability and durability
* Provides a customizable and extensible architecture for easy integration with other data sources and tools
* The project is still in it's development and for the near future should have a ML feature using xgboost to predict future fixtures.

## 🛠️ Tech Stack
* Apache Airflow for workflow management
* Apache Spark for data processing
* Amazon S3 for data storage (boto3)
* Python 3.11 for scripting and development
* Docker for containerization and deployment
* LocalStack for mocking AWS services in development environment
* Bitnami Spark for Spark cluster deployment

## 📦 Installation
To install the project, follow these steps:
1. Clone the repository using `git clone`
2. Install Docker and Docker Compose on your machine
3. Run `docker-compose up` to start the services
4. Install Apache Airflow and Apache Spark on your machine
5. Configure the `core-site.xml` file to point to your S3 bucket

## 💻 Usage
To use the project, follow these steps:
1. Start the services using `docker-compose up`
2. Configure the Airflow DAGs to point to your S3 bucket (I used boto3)
3. Run the `fixtures_to_bronze_dag` to fetch data from the football API
4. Run the `bronze_to_silver_dag` to transform raw data to silver layer
5. Run the `silver_to_feature_dag` to process data to feature layer

## 📂 Project Structure
```markdown
.
├── airflow
│   ├── dags
│   │   ├── bronze_to_silver_dag.py
│   │   ├── fixtures_to_bronze_dag.py
│   │   └── silver_to_feature_dag.py
│   └── plugins
│       └── hooks
│           └── football_api_hook.py
├── spark
│   ├── jobs
│   │   └── silver_to_features.py
│   └── conf
│       └── core-site.xml
├── infra
│   └── docker-compose.yml
└── README.md
```

## 🤝 Contributing
To contribute to the project, please follow these steps:
1. Fork the repository using `git fork`
2. Create a new branch using `git branch`
3. Make changes and commit them using `git commit`
4. Push the changes to your fork using `git push`
5. Create a pull request to merge the changes

## 📬 Contact
For any questions or concerns, please contact me at [gabrieldantasfgs@gmail.com](mailto:gabrieldantasfgs@gmail.com).
