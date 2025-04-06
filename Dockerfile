# Используем официальный образ Python
FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы зависимостей
COPY requirements.txt ./

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем файлы проекта
COPY . .

# Устанавливаем переменную окружения для запуска Python без буферизации
ENV PYTHONUNBUFFERED=1

# Запускаем бота
CMD ["python", "bot.py"] 