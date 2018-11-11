import asyncio
import os
import re

id_matcher = re.compile("(?<=/)\d+(?=/)")


async def main():
    files = os.listdir("links")
    for file in files:
        with open(f"links/{file}", encoding="utf-8") as link_file:
            links = link_file.readlines()
            for link in links:
                nlink = link.replace("\n", "")
                link_id = id_matcher.search(nlink).group(0)
                folder = file.replace(".txt", "")
                if not os.path.exists(f"images/{folder}"):
                    os.mkdir(f"images/{folder}")
                print(f"Downloading {link_id} to folder {folder}",end="\r")
                await ugetter(file.replace(".txt", ""), f"{link_id}.png", nlink)



async def ugetter(folder, filename, url):
    cmd = [
        "wget",
        url,
        "-O",
        f"images/{folder}/{filename}",
        "-q"
    ]
    process = await asyncio.create_subprocess_exec(*cmd)
    return await process.wait()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
