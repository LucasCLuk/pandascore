import asyncio
import functools
import typing
import ujson
from typing import Optional

import dateutil.parser
import firebase_admin
from firebase_admin import credentials

import firestore
import pandascore


class Reader:

    def __init__(self, loop, token, fb_app):
        self.loop = loop
        self.panda = pandascore.PandaScore(loop, token)
        self.firebase = firestore.FireBaseManager(fb_app)
        self.finished = asyncio.Event()
        self.tasks: typing.List[bool] = []
        self.task_amount = 0
        self.links: typing.Dict[str: typing.List[str]] = {}

    @staticmethod
    def format_key(key):
        if key == "serie":
            return "series"
        elif key == "serie_id":
            return "seriesId"
        elif "_" in key:
            p_case = key.split("_")
            return f"{p_case[0]}{p_case[1].title()}"
        else:
            return key

    async def process_image(self, group_name, data, url) -> Optional[str]:
        entity_id = data['id']
        url_data = await self.panda.get_data_from_url(url)
        label = self.panda.get_image_ext(url)
        blob_data = self.firebase.get_blob_url(group_name, label)
        if blob_data:
            return blob_data
        if url_data is not None:
            image_bytes, image_ext = url_data
            if image_bytes is not None:
                print(f"Uploading {group_name}/{entity_id}.{image_ext}")
                bucket_path = f"{entity_id}.{image_ext}"
                metadata = {
                    group_name: data['slug']
                }
                fb_url = self.firebase.upload_image(group_name, bucket_path, image_bytes, metadata)
                return fb_url

    async def process_entry(self, group_name, data):
        try:
            formatted_data = await self.process_dict(data, group_name)
            print(f"Uploading {formatted_data['id']} to firebase")
            wrapper = functools.partial(self.firebase.upload_data, group_name, formatted_data['id'], formatted_data)
            await event_loop.run_in_executor(None, wrapper)
        finally:
            self.tasks.append(True)

    async def run(self):
        data = await self.panda.get_all_data()
        self.task_amount = sum(len(_data) for _data in data.values())
        for entry, data in data.items():
            for data_entry in data:
                self.loop.create_task(self.process_entry(entry, data_entry))
        self.loop.create_task(self.task_manager())
        await self.finished.wait()
        for group, links in self.links.items():
            undups = set(links)
            with open(f"links/{group}.txt", "w+") as link_file:
                link_file.writelines("\n".join(undups))

    async def run_blocking(self):
        data = await self.panda.get_all_data()
        self.task_amount = sum(len(_data) for _data in data.values())
        for entry, data in data.items():
            for data_entry in data:
                await self.process_entry(entry, data_entry)
        for group, links in self.links.items():
            undups = set(links)
            with open(f"links/{group}.txt", "w+") as link_file:
                link_file.writelines("\n".join(undups))

    async def process_dict(self, data: dict, group_name) -> dict:
        formatted_dict = {}
        for key, value in dict(data).items():
            formatted_key = self.format_key(key)
            if isinstance(value, dict):
                formatted_dict[formatted_key] = await self.process_dict(value, key)
            elif isinstance(value, list):
                copy_value = list(value)
                formatted_dict[key] = []
                for index, entry in enumerate(copy_value):
                    formatted_dict[key].append(await self.process_dict(entry, key))
            elif key == "image_url" and value is not None:
                formatted_dict[formatted_key] = await self.process_image_link(group_name, data, value)
            elif "at" in key and value is not None:
                try:
                    formatted_dict[formatted_key] = dateutil.parser.parse(value)
                except ValueError:
                    formatted_dict[formatted_key] = value
            else:
                formatted_dict[formatted_key] = value
        return formatted_dict

    async def task_manager(self):
        while True:
            if len(self.tasks) == self.task_amount:
                self.finished.set()
                break
            await asyncio.sleep(60)

    async def process_image_link(self, group_name, data, value):
        links = self.links.get(group_name)
        if links:
            links.append(value)
        else:
            self.links[group_name] = [value]
        entity_id = data['id']
        image_ext = self.panda.get_image_ext(value)
        bucket_path = f"{entity_id}.{image_ext}"
        fb_url = self.firebase.set_blob_url(group_name, bucket_path)
        return fb_url


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    with open("pandacredentials.json") as file:
        token_data = ujson.loads(file.read())

    cred = credentials.Certificate("firecredentials.json")
    gc_fb_app = firebase_admin.initialize_app(cred, {
        "storageBucket": "onleague-3cb5e.appspot.com"
    })
    reader = Reader(event_loop, token_data['token'], gc_fb_app)
    event_loop.run_until_complete(reader.run_blocking())
