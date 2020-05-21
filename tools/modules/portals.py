import aiohttp
import asyncio
import os
import json

from aiohttp import ClientSession


PORTALS_URL = "http://201.73.128.131:8080/CIP/metadata/search/portals-general-access/situatedviews?table=AssetRecords&quicksearchstring="
CREATORS = [
    "Marc Ferrez",
    "Augusto Malta",
]


def extract_fields_from_response(response):
    """Extract fields from API's response"""
    item = response.get("items", [{}])[0]
    # identifier = item.get("volumeInfo", {})
    record_name = item.get("RecordName", None)
    portals_id = item.get("id", None)
    # description = volume_info.get("description", None)
    # published_date = volume_info.get("publishedDate", None)
    return (
        record_name,
        portals_id,
    )


async def get_items_async(creator, session):
    """Search published items using Cumulus Portals API (asynchronously)"""
    url = PORTALS_URL + creator
    try:
        response = await session.request(method="POST", url=url)
        response.raise_for_status()
        print(f"Response status ({url}): {response.status}")
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error ocurred: {err}")
    response_json = await response.json()
    return response_json


async def run_program(creator, session):
    """Wrapper for running program in an asynchronous manner"""
    try:
        response = await get_items_async(creator, session)
        parsed_response = extract_fields_from_response(response)
        print(f"Response: {json.dumps(parsed_response, indent=2)}")
    except Exception as err:
        print(f"Exception occured: {err}")
        pass


async def do_it():
    async with ClientSession() as session:
        await asyncio.gather(*[run_program(creator, session) for creator in CREATORS])


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(do_it())
    loop.close()


if __name__ == "__main__":
    main()
