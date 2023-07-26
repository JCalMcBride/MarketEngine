import json
import lzma

import aiohttp
import redis

from ..Common import fetch_api_data, config

MANIFEST_URL = "https://content.warframe.com/PublicExport/index_en.txt.lzma"  # Contains the URLs for each manifest


def decompress_lzma(data: bytes) -> bytes:
    """
    Decompresses LZMA data.
    :param data: The LZMA-compressed data.
    :return: The decompressed data.
    """
    results = []
    while True:
        decompressed_data = lzma.LZMADecompressor(lzma.FORMAT_AUTO, None, None)
        try:
            result = decompressed_data.decompress(data)
        except lzma.LZMAError:
            if results:
                break  # Leftover data is not a valid LZMA/XZ stream; ignore it.
            else:
                raise  # Error on the first iteration; bail out.
        results.append(result)
        data = decompressed_data.unused_data
        if not data:
            break
        if not decompressed_data.eof:
            raise lzma.LZMAError("Compressed data ended before the end-of-stream marker was reached")
    return b"".join(results)


async def fetch_base_manifest(cache: redis.Redis,
                              session: aiohttp.ClientSession) -> str:
    """
    Fetches the base manifest from the Warframe CDN.
    :param cache: The Redis cache.
    :param session: The aiohttp session.
    :return: The base manifest.
    """
    data = await fetch_api_data(cache=cache,
                                session=session,
                                url=MANIFEST_URL,
                                return_type='bytes')

    byt = bytes(data)
    length = len(data)
    stay = True
    while stay:
        stay = False
        try:
            decompress_lzma(byt[0:length])
        except lzma.LZMAError:
            length -= 1
            stay = True

    return decompress_lzma(byt[0:length]).decode("utf-8")


def save_manifest(manifest_dict: dict):
    """
    Saves the manifest to a file.
    :param manifest_dict: The manifest.
    :return: None
    """
    for item in manifest_dict:
        with open(f"{config['output_dir']}/manifest_{item}.json", "w") as f:
            json.dump(manifest_dict[item], f)


async def get_manifest(cache: redis.Redis,
                       session: aiohttp.ClientSession):
    """
    Fetches each manifest from the Warframe CDN, and returns a dictionary containing each manifest.
    :param cache: The Redis cache.
    :param session: The aiohttp session.
    :return: The manifest dictionary.
    """
    wf_manifest = await fetch_base_manifest(cache, session)
    wf_manifest = wf_manifest.split('\r\n')

    manifest_dict = {}
    for item in wf_manifest[:-1]:
        url = f"http://content.warframe.com/PublicExport/Manifest/{item}"

        data = await fetch_api_data(cache=cache,
                                    session=session,
                                    url=url,
                                    return_type='text')

        json_file = json.loads(data, strict=False)

        manifest_dict[item.split("_en")[0]] = json_file

    return manifest_dict
