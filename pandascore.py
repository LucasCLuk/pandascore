import typing

import aiohttp
import ujson
import asyncio
import io


class PandaScore:
    base_url = "https://api.pandascore.co/lol"
    endpoints = {
        "leagues": "/leagues",
        # "series": "/series",
        # "tournaments": "/tournaments",
        # "players": "/players",
        # "teams": "/teams",
    }

    def __init__(self, loop: asyncio.AbstractEventLoop, token: str):
        self.session = aiohttp.ClientSession(loop=loop, json_serialize=ujson.dumps,
                                             headers={"Authorization": f"Bearer {token}"})

    async def _get_response(self, endpoint: str, page: int = 1) -> typing.List:
        data = []
        try:
            async with self.session.get(f"{self.base_url}{endpoint}",
                                        params={"page": page, "per_page": 100}) as response:
                if response.status == 200:
                    print(f"{endpoint}: Page: {page}")
                    response_data = await response.json()
                    has_next = len(response_data) > 0
                    if has_next:
                        data.extend(response_data)
                else:
                    print(f"Response returned status {response.status}")
                    try:
                        error_data = await response.json()
                        print(error_data)
                    except:
                        pass
                    return []
            if has_next:
                next_batch = await self._get_response(endpoint, page + 1)
                if next_batch is not None:
                    data.extend(next_batch)
            return data
        except:
            return []

    async def get_leagues(self) -> list:
        return await self._get_response(self.endpoints['leagues'])

    async def get_series(self) -> list:
        return await self._get_response(self.endpoints['series'])

    async def get_tournaments(self) -> list:
        return await self._get_response(self.endpoints['tournaments'])

    async def get_matches(self) -> list:
        return await self._get_response(self.endpoints['matches'])

    async def get_players(self) -> list:
        return await self._get_response(self.endpoints['players'])

    async def get_teams(self) -> list:
        return await self._get_response(self.endpoints['teams'])

    async def get_all_data(self) -> typing.Dict[str, typing.Any]:
        data = {}
        for key, endpoint in self.endpoints.items():
            data[key] = await self._get_response(endpoint)
        return data

    async def get_data_from_url(self, url: str) -> typing.Optional[typing.Tuple[io.BytesIO, str]]:
        try:
            sleep_counter = 10
            attempts = 0
            while True:
                print(f"Getting Data from {url}")
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return io.BytesIO(await response.read()), url.split(".")[-1]
                    else:
                        print(f"Unable to get data, response returned: {response.status}")
                        sleep_counter *= 2
                        if sleep_counter > 240:
                            sleep_counter = 10
                        attempts += 1
                        if attempts > 40:
                            return
                        await asyncio.sleep(sleep_counter)
        except:
            print(f"Unable to get data from {url}")
            return

    @staticmethod
    def get_image_ext(url: str) -> str:
        return url.split(".")[-1]
