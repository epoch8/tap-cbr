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


# https://exchangerate.host/documentation
# INFO {"access_key": "<DELETED>", "source": "USD", "date": "2023-09-27"}
# {'success': True, 'terms': 'https://currencylayer.com/terms', 'privacy': 'https://currencylayer.com/privacy', 'historical': True, 'date': '2023-09-27', 'timestamp': 1695859199, 'source': 'USD', 'quotes': {'USDAED': 3.672945, 'USDAFN': 77.000256, 'USDALL': 100.318606, 'USDAMD': 388.589713, 'USDANG': 1.804058, 'USDAOA': 829.000263, 'USDARS': 349.988404, 'USDAUD': 1.575423, 'USDAWG': 1.8, 'USDAZN': 1.697825, 'USDBAM': 1.855079, 'USDBBD': 2.02116, 'USDBDT': 110.387609, 'USDBGN': 1.861885, 'USDBHD': 0.376945, 'USDBIF': 2847.5, 'USDBMD': 1, 'USDBND': 1.372226, 'USDBOB': 6.917091, 'USDBRL': 5.043697, 'USDBSD': 1.001001, 'USDBTC': 3.7953509e-05, 'USDBTN': 83.296927, 'USDBWP': 13.789113, 'USDBYN': 2.526843, 'USDBYR': 19600, 'USDBZD': 2.017813, 'USDCAD': 1.350155, 'USDCDF': 2495.999939, 'USDCHF': 0.92088, 'USDCLF': 0.032948, 'USDCLP': 909.129666, 'USDCNY': 7.309104, 'USDCOP': 4104.3, 'USDCRC': 539.00763, 'USDCUC': 1, 'USDCUP': 26.5, 'USDCVE': 105.349667, 'USDCZK': 23.175963, 'USDDJF': 177.720343, 'USDDKK': 7.09742, 'USDDOP': 56.801353, 'USDDZD': 137.548493, 'USDEGP': 30.897993, 'USDERN': 15, 'USDETB': 55.244964, 'USDEUR': 0.95194, 'USDFJD': 2.306903, 'USDFKP': 0.82207, 'USDGBP': 0.824075, 'USDGEL': 2.67503, 'USDGGP': 0.82207, 'USDGHS': 11.575036, 'USDGIP': 0.82207, 'USDGMD': 64.99994, 'USDGNF': 8650.000102, 'USDGTQ': 7.868392, 'USDGYD': 209.428603, 'USDHKD': 7.821435, 'USDHNL': 24.824974, 'USDHRK': 7.014709, 'USDHTG': 135.638714, 'USDHUF': 373.730342, 'USDIDR': 15576, 'USDILS': 3.840251, 'USDIMP': 0.82207, 'USDINR': 83.23615, 'USDIQD': 1310, 'USDIRR': 42237.501321, 'USDISK': 137.940208, 'USDJEP': 0.82207, 'USDJMD': 154.769075, 'USDJOD': 0.709803, 'USDJPY': 149.503998, 'USDKES': 147.950184, 'USDKGS': 88.709717, 'USDKHR': 4122.999924, 'USDKMF': 469.350137, 'USDKPW': 900.024128, 'USDKRW': 1353.819681, 'USDKWD': 0.30931, 'USDKYD': 0.834195, 'USDKZT': 479.957886, 'USDLAK': 20199.99989, 'USDLBP': 15035.00038, 'USDLKR': 324.200762, 'USDLRD': 186.650227, 'USDLSL': 19.189964, 'USDLTL': 2.95274, 'USDLVL': 0.60489, 'USDLYD': 4.87503, 'USDMAD': 10.32375, 'USDMDL': 18.169057, 'USDMGA': 4524.999949, 'USDMKD': 58.564561, 'USDMMK': 2102.227997, 'USDMNT': 3470.16843, 'USDMOP': 8.062017, 'USDMRO': 356.999828, 'USDMUR': 44.490981, 'USDMVR': 15.454979, 'USDMWK': 1082.502223, 'USDMXN': 17.69015, 'USDMYR': 4.708019, 'USDMZN': 63.24995, 'USDNAD': 19.189974, 'USDNGN': 781.516746, 'USDNIO': 36.530085, 'USDNOK': 10.76023, 'USDNPR': 133.272632, 'USDNZD': 1.690425, 'USDOMR': 0.38501, 'USDPAB': 1.001001, 'USDPEN': 3.792502, 'USDPGK': 3.631502, 'USDPHP': 57.006499, 'USDPKR': 288.725012, 'USDPLN': 4.410603, 'USDPYG': 7296.473166, 'USDQAR': 3.641053, 'USDRON': 4.735695, 'USDRSD': 111.652026, 'USDRUB': 97.070241, 'USDRWF': 1212.5, 'USDSAR': 3.750702, 'USDSBD': 8.36952, 'USDSCR': 14.149699, 'USDSDG': 600.902262, 'USDSEK': 11.065085, 'USDSGD': 1.37299, 'USDSHP': 1.21675, 'USDSLE': 22.688915, 'USDSLL': 19750.000319, 'USDSOS': 569.502312, 'USDSRD': 38.214499, 'USDSTD': 20697.981008, 'USDSSP': 600.520749, 'USDSYP': 13002.104008, 'USDSZL': 19.19015, 'USDTHB': 36.679751, 'USDTJS': 10.996576, 'USDTMT': 3.505, 'USDTND': 3.175999, 'USDTOP': 2.39165, 'USDTRY': 27.337802, 'USDTTD': 6.79474, 'USDTWD': 32.240801, 'USDTZS': 2510.000238, 'USDUAH': 36.961615, 'USDUGX': 3758.838677, 'USDUYU': 38.337688, 'USDUZS': 12214.999758, 'USDVEF': 3396221.384444, 'USDVES': 34.207913, 'USDVND': 24400, 'USDVUV': 121.726568, 'USDWST': 2.778715, 'USDXAF': 622.146451, 'USDXAG': 0.044342, 'USDXAU': 0.000533, 'USDXCD': 2.70255, 'USDXDR': 0.760709, 'USDXOF': 620.507894, 'USDXPF': 113.34999, 'USDYER': 250.400803, 'USDZAR': 19.207399, 'USDZMK': 9001.192558, 'USDZMW': 20.74653, 'USDZWL': 321.999592}}

endpoint = "http://api.exchangerate.host/historical"
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


def do_sync(access_key, source, date_start: str, date_stop: str, currencies: Optional[list] = None) -> Optional[str]:

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
                if response.status_code != 200:
                    delay = delay_seconds * 2 ** retry
                    logger.info(f'Response URL:\t{response.url}')
                    logger.info(f'Response status code:\t{response.status_code}')
                    logger.info(f'Response text:\t{response.text}')
                    logger.info(f'Seconds before next retry:\t{delay}')
                    time.sleep(delay)
                else:
                    return response
            
        logger.warning(f'Failed after {n_retries} attempt(s)!')

    date_to_process = date_start
    data = []
    state = {
        'date_start': date_start,
        'date_stop': date_stop
    }

    while date.fromisoformat(date_to_process) <= date.fromisoformat(date_stop):
        params = {
            "access_key": access_key,
            "source": source,
            "date": date_to_process
        }

        if currencies:
            params['currencies'] = ','.join(currencies)

        logger.info(json.dumps(params))

        response = make_retry(
            url=endpoint,
            params=params,
            n_retries=N_RETRIES,
            delay_seconds=DELAY_SECONDS
        )

        if response:
            record = response.json().get('quotes')
            if record:
                record[source] = 1.0
                record['date'] = date_to_process
                data = data + [record]

        date_to_process = (date.fromisoformat(date_to_process) + timedelta(days=1)).strftime(DATE_FORMAT)

    if record:
        singer.write_schema("exchange_rate", make_schema(record), "date")
        for record in data:
            singer.write_records("exchange_rate", [record])
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

    do_sync(config.get("access_key"), config.get("source", "USD"), date_start, date_stop, config.get("currencies"))


if __name__ == "__main__":
    main()
