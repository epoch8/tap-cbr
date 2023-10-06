#!/usr/bin/env python3
from __future__ import annotations
import argparse
from datetime import timedelta
from datetime import date
import json
import time
from typing import Optional
import requests
import singer


# https://www.cbr-xml-daily.ru//archive//2023//10//05//daily_json.js
# Response :
# {
#     "Date": "2023-10-05T11:30:00+03:00",
#     "PreviousDate": "2023-10-04T11:30:00+03:00",
#     "PreviousURL": "\/\/www.cbr-xml-daily.ru\/archive\/2023\/10\/04\/daily_json.js",
#     "Timestamp": "2023-10-05T14:00:00+03:00",
#     "Valute": {
#         "USD": {
#             "ID": "R01235",
#             "NumCode": "840",
#             "CharCode": "USD",
#             "Nominal": 1,
#             "Name": "Р”РѕР»Р»Р°СЂ РЎРЁРђ",
#             "Value": 99.4555,
#             "Previous": 99.2677
#         },
#         "EUR": {
#             "ID": "R01239",
#             "NumCode": "978",
#             "CharCode": "EUR",
#             "Nominal": 1,
#             "Name": "Р•РІСЂРѕ",
#             "Value": 104.3024,
#             "Previous": 104.0621
#         },
#         ...
#     }
# }


ENDPOINT = r"https://www.cbr-xml-daily.ru//archive//DATE//daily_json.js"
logger = singer.get_logger()

DATE_FORMAT = "%Y-%m-%d"
N_RETRIES = 10
DELAY_SECONDS = 10


def make_schema(record: dict) -> dict:
    # Make Singer schema
    schema = {
        "type": "object",
        "properties": {
            "date": {
                "type": "string",
                "format": "date",
            }
        }
    }
    # Populate the currencies
    for rate in record:
        if rate not in schema["properties"]:
            # noinspection PyTypeChecker
            schema["properties"][rate] = {"type": ["null", "number"]}
    return schema


def do_sync(date_start: str, date_stop: str, currencies: Optional[list] = None) -> Optional[str]:

    def make_retry(url, params, n_retries, delay_seconds):
        for retry in range(n_retries):
            try:
                response = requests.request('get', url, params=params)
            except Exception as e:
                logger.info(e)
                delay = delay_seconds * 2 ** retry
                logger.info(f'Seconds before next retry:\t{delay}')
                time.sleep(delay)
            else:
                if response.status_code != 200 and 'Курс ЦБ РФ на данную дату не установлен или указана ошибочная дата.' not in response.text:
                    delay = delay_seconds * 2 ** retry
                    logger.info(f'Response URL:\t{response.url}')
                    logger.info(f'Response status code:\t{response.status_code}')
                    logger.info(f'Response text:\t{response.text}')
                    logger.info(f'Seconds before next retry:\t{delay}')
                    time.sleep(delay)
                elif 'Курс ЦБ РФ на данную дату не установлен или указана ошибочная дата.' in response.text:
                    logger.warning(f'Response text:\t{response.text}')
                    return None
                else:
                    return response
            
        logger.warning(f'Failed after {n_retries} attempt(s)!')

    if type(currencies) != list:
        logger.warning(f'Setting "currencies" is not a list! Ignoring.')
        currencies = None
    else:
        logger.info(f'Currencies specified:\n{currencies}')

    date_to_process = date_start
    data = []
    state = {
        'date_start': date_start,
        'date_stop': date_stop
    }

    while date.fromisoformat(date_to_process) <= date.fromisoformat(date_stop):
        time.sleep(2)
        logger.info(f'Date to process:\t{date_to_process}')

        endpoint = ENDPOINT.replace('DATE', date.fromisoformat(date_to_process).strftime('%Y//%m//%d'))

        response = make_retry(
            url=endpoint,
            params=None,
            n_retries=N_RETRIES,
            delay_seconds=DELAY_SECONDS
        )

        if response:
            valutes = response.json().get('Valute')
            if valutes:
                record = {'date': date_to_process}
                if currencies:
                    for valute in currencies:
                        record[valute] = valutes.get(valute, {}).get('Value')
                        record[f'{valute}_Nominal'] = valutes.get(valute, {}).get('Nominal')
                else:
                    for valute in valutes:
                        record[valute] = valutes[valute].get('Value')
                        record[f'{valute}_Nominal'] = valutes[valute].get('Nominal')
                data = data + [record]

        date_to_process = (date.fromisoformat(date_to_process) + timedelta(days=1)).strftime(DATE_FORMAT)

    if len(data) > 0:
        singer.write_schema("exchange_rate_cbr", make_schema(record), "date")
        for record in data:
            singer.write_records("exchange_rate_cbr", [record])
            state['date_start'] = (date.fromisoformat(record['date']) + timedelta(days=1)).strftime(DATE_FORMAT)
        
        state['date_stop'] = (date.fromisoformat(state['date_stop']) + timedelta(days=1)).strftime(DATE_FORMAT)
        singer.write_state(state)
        logger.info(json.dumps(
            {"message": f"tap completed successfully rows={len(data)}"}
        ))
    else:
        logger.info(json.dumps(
            {"message": "tap completed successfully (nothing done, no new data)."}
        ))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c", "--config", help="Config file", required=False)
    parser.add_argument(
        "-s", "--state", help="State file", required=False)

    args = parser.parse_args()

    if args.config:
        with open(args.config) as f:
            config = json.load(f)
    else:
        config = {}

    if args.state:
        with open(args.state) as f:
            state = json.load(f)
    else:
        state = {}

    date_start = (
        config.get("date_start")
        or state.get("date_start")
        or (date.today() - timedelta(days=1)).strftime(DATE_FORMAT)
    )
    
    date_stop = (
        config.get("date_stop")
        or state.get("date_stop")
        or (date.today() - timedelta(days=1)).strftime(DATE_FORMAT)
    )

    do_sync(date_start, date_stop, config.get("currencies"))


if __name__ == "__main__":
    main()
