import requests
headers = {
       
    'X-Nickname': 'Artem-Merkulov',
    'X-Cohort': '20',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'

}
response_1 = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field=id&sort_direction=asc&limit=50&offset=0', headers=headers);
response_2 = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=id&sort_direction=asc&limit=10&offset=0', headers=headers);
response_3 = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?from=2022-01-01 00:00:00&to=2500-01-01 00:00:00&sort_field=date&sort_direction=asc&limit=10&offset=0', headers=headers);

print(response_1.text);
print(response_2.text);
print(response_3.text);