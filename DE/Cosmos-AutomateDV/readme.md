# Инструкция по запуску проекта Cosmos-AutomateDV

```
Cosmos-AutomateDV
├───.astro
└───dags
    ├───csv
    ├───dbt
    │   └───AutoDV
    │       ├───dbt_packages
    │       ├───logs
    │       ├───models
    │       │   ├───raw_stage
    │       │   ├───raw_vault
    │       │   │   ├───hubs
    │       │   │   ├───links
    │       │   │   └───sats
    │       │   └───stage
    │       └───target
    └───sql
```

## Для развертывания проекта необходимо:
1. Python 3.11.8 (python --version).
2. IDE ([VSCode](https://code.visualstudio.com/), [PyCharm](https://www.jetbrains.com/pycharm/)).
3. Расширения python, jupyter и DBT для VSCode.
4. Проект реализован на ОС Windows и все команды приведены для ОС Windows.
5. Проект реализован на СУБД PostgreSQL, установленной локально.
6. [Видео](https://youtu.be/1fY1A8SRflI?si=j_RLf5bczqdu0SyR) для инициализации DBT-проекта.

## Порядок запуска проекта Cosmos-AutomateDV:
1. Установить зависимости для Astronomer-Cosmos и DBT - `pip-freeze` через `requirements.txt`.
2. Добавить csv-файлы в директорию `Cosmos-AutomateDV\dags\csv`.
3. Запустить в Airflow `autodv-init`.
4. Запустить в Airflow `autodv-dag`.

## Создание DBT-проекта:
###### Информация для справки.
1. Создать директорию (Например AutoDV), в которой будет развёрнут проект и перейти в неё. 
   Открываю дирейторию в VSCode для удобства.
2. Создать виртуальную среду python: ```python -m venv dbt_venv```
3. Активировать созданную виртуальную среду: ```.\dbt_venv\Scripts\Activate.ps1```
4. Установить python-библиотеки из requirements.txt: ```pip install -r requirements.txt``` Получить список зависимостей для текущего виртуального окружения можно используя ```pip freeze```
5. Установить dbt для определённой СУБД - в этом проекте PostgreSQL: ```pip install dbt-postgres```
6. Возможно нужно будет обновить pip: ```python.exe -m pip install --upgrade pip```
7. В создать директорию .dbt в папке пользователя Windows (терминале Windows (cmd)): ```cd %USERPROFILE% mkdir .dbt```
8. В терминале VSCode (в директории проекта AutomateDV) инициализировать DBT-проект: ```dbt init```
9. Ввести имя проекта (Например AutomateDV): ```AutoDV```
10. Выбрать СУБД Postgres: ```1```
11. Прописать host СУБД (например localhost): ```localhost```
12. Прописать порт СУБД (например 5432): ```5433```
13. Прописать user СУБД (например admin): ```postgres```
14. Прописать pass СУБД (например StrongPassword): ```**************```
15. Прописать dbname СУБД (например mydb): ```postgres```
16. Прописать schema СУБД в которой будет выполняться проект (например auto_dv): ```auto_dv```
17. Прописать threads DBT (например 1): ```4```
    В директории .dbt создался файл profile.yml с параметрами подключения к СУБД.
18. Проверить работоспособность DBT-проекта: ```dbt debug```

## Добавление проекта AutomateDV в созданный DBT проект:
###### Информация для справки.
   Как работать с пакетом AutomateDV можно посмотреть по [ссылке](https://www.youtube.com/@AutomateDV). 
1. Скопировать файл packajes.yml, который находится по пути ```\AutoDV\AutoDV\packajes.yml``` в директорию проекта на один уровень с файлом ```dbt_project.yml```.
2. В терминале VSCode выполнить команду (на уровне ```dbt_project.yml```): ```dbt deps```
3. Заменить исходную директорию ```models``` на директорию ```models``` из репозитория с проектом из git (со всем содержимым).
4. Из ```models``` в ```dbt_project``` можно убрать ```example```.
5. Проверяем проект на наличие ошибок: ```dbt debug```
6. Компилируем манифест: ```dbt compile```
7. Генерируем автодокументацию: ```dbt docs generate```
8. Переходим к документайции: ```dbt docs serve```
9. В правом нижнем углу нажимаем на кружок, чтобы увидеть ```Lineage Graph``` выполнения моделей.
10. Создаём объекты workflow в СУБД и заполняем их данными за 2004-08-30: ```dbt run --vars '{load_dt: 2004-08-30, load_src: 1C}'```. Здесь ```2004-08-30``` - Дата первого отправленного сообщения в таблице фактов - ```dialog```. А ```1C``` - источник данных.
11. Далее последовательно меняем дату в переменной ```load_dt``` для инкрементального заполнения элементов DataVault.

![Lineage Graph](https://drive.google.com/uc?id=1FXNTcZRlILZPFCSvOE7dvRFynofA0Gft)

## Добавление оркестрации Airflow от Astronomer-Cosmos:
###### Информация для справки.
1. [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/index.html)
2. Как создать оркестрацию с помощью Cosmos можно посмотреть по [ссылке](https://youtu.be/MhCuxTDlVkE?si=-3987OHfako26Xtq).
3. Файлы, относящиеся к `Cosmos` располагаются в директории `.astro`. DBT-проект, который мы хотим оркестрировать, должен быть расположен в директории `dbt`:
```
Cosmos-AutomateDV
├───.astro
└───dags
    ├───csv
    └───dbt
        └───AutoDV <- Добавили сюда AutoDV.
```

## Создание источников в базе данных:
1. Переместить csv-файлы в директорию `.\Cosmos-AutomateDV\dags\csv`.
2. В Airflow запустить `autodv-init`.

## В Airflow workflow будет выглядеть следующим образом:
#### autodv-init
![Init](https://drive.google.com/uc?export=download&id=1V_4u1WfZUJcRWz-3E11aOCZRCz4Da_68)
#### autodv-dag
![Workflow](https://drive.google.com/uc?id=1XqOPXAddEdwCDsKrQhOAYj4gxJNmLhZq)