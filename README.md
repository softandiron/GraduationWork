## Дипломная работа в Нетологии
### По курсу: Продвинутый инжиниринг данных
### Работу выполнил: Минин Артём
#
### Задание:

Вам необходимо построить airflow пайплайн выгрузки ежедневных отчётов по количеству поездок на велосипедах в городе Нью-Йорк.

Рекомендации при выполнении работы:

Пайплайн должен состоять из следующих шагов:

Отслеживание появление новых файлов в своём бакете на AWS S3. Представим, что пользователь или провайдер данных будет загружать новые исторические данные по поездкам в Ваш бакет;
При появлении нового файла запускается оператор импорта данных в созданную таблицу базы данных Clickhouse;
Необходимо сформировать таблицы с ежедневными отчётами по следующим критериям:
– количество поездок в день
– средняя продолжительность поездок в день
– распределение поездок пользователей, разбитых по категории «gender»
Данные статистики необходимо загрузить на специальный S3 бакет с хранящимися отчётами по загруженным файлам.

### Решение:
В ходе выполнения выпускной работы на виртуальную машину с операционной системой Linux Mint был установлен Apache Airflow и Scheduler.
Так же был создан аккаунт Amazon AWS S3 с двумя бакетами: 'netobucket' и 'netobucketreports'.
В первом хранятся исходные данные (появляющиеся данные по поездкам), а во второй бакет будут загружаться отчёты.

Для решения задачи на той же виртуальной машине был запущен сервер Clickhouse.
В качестве редактора кода была установленна IDE PyCharm.

В IDE был создан DAG, который после нескольких переделок получил название S3_ETL_v3.py

В указанном Даге выполняется только одна задача, представляющая собой функцию типа PythonOperator
Данная функция называется ETL и включает набор подфункций, снабжённых комментариями.

Пайплайн состоит из следующих этапов:
- чтение ключей доступа к AWS S3 из секретного файла
- получение списка файлов в исходном бакете
- поиск в списке новых файлов. Список уже обработанных файлов хранится в Airflow Variables

Далее для каждого нового файла:
- скачивание файла (.zip)
- разархивирование (.csv)
- создание Clickhouse таблицы по названию файла с необходимыми колонками
- загрузка данных в ClickHouse таблицу через pandas
- получение отчётов в Clickhouse путём SQL запросов
- сбор отчётов в pandas Data Frame
- экспорт отчётов в .csv файл
- загрузка отчётов на бакет с отчётами
- удаление таблицы

В конце происходит обновление Variables со списком обработанных файлов.

![My ETL scheme](https://github.com/softandiron/GraduationWork/blob/main/My%20ETL.jpeg)


### Как пользоваться
Для начала работы нужно создать в основной директории программы файл `config.py`
В файле прописать необходимые данные:


DB_NAME = 'tripDB'

HOST = 'http://localhost:8123'

USER = 'default'

PASSWORD = ''

FROM_BUCKET = 'netobucket'

TO_BUCKET = 'netobucketreports'

ACCESS_KEY = 'long key'

SECRET_KEY = 'another long key'




