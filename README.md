Проект ETL_PostgreSQL_Elasticsearch

Описание: Отдельный микросервис. ETL процесс для переноса данных из базы данных
PostgrSQL в Elasticsearch. Для верификации данных использованы модели Pydantic.

Технологии: Python, SQL, Pydantic, PostgreSQL, Elasticsearch, Docker

Для запуска сервиса:
1. Переменовать файл .env.dev в .env
2. Переименовать файл docker-compose-dev.yml в docker-compose.yml
3. Переименовать файл etl/src/etl.env.dev в etl/src/etl.env
4. Выполнить команду docker compose up --build
