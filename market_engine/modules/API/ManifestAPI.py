import lzma

from market_engine.common import fetch_api_data

MANIFEST_URL = "https://content.warframe.com/PublicExport/index_en.txt.lzma"

def decompress_lzma(data):
    results = []
    while True:
        decomp = lzma.LZMADecompressor(lzma.FORMAT_AUTO, None, None)
        try:
            res = decomp.decompress(data)
        except lzma.LZMAError:
            if results:
                break  # Leftover data is not a valid LZMA/XZ stream; ignore it.
            else:
                raise  # Error on the first iteration; bail out.
        results.append(res)
        data = decomp.unused_data
        if not data:
            break
        if not decomp.eof:
            raise lzma.LZMAError("Compressed data ended before the end-of-stream marker was reached")
    return b"".join(results)


async def fix(cache, session):
    data = await fetch_api_data(MANIFEST_URL, cache, session)

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


def save_manifest(manifest_dict):
    for item in manifest_dict:
        with open(f"data/manifest_{item}.json", "w") as f:
            json.dump(manifest_dict[item], f)


async def get_manifest():
    async with session_manager() as session, cache_manager() as cache:
        wf_manifest = await fix(cache, session)
        wf_manifest = wf_manifest.split('\r\n')
        manifest_dict = {}
        for item in wf_manifest:
            try:
                url = f"http://content.warframe.com/PublicExport/Manifest/{item}"
                data = get_cached_data(cache, url)
                if data is None:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        data = await response.text()
                        logger.debug(f"Fetched data for {url}")

                        # Store the data in the cache with a 24-hour expiration
                        cache.set(url, data, ex=24 * 60 * 60)

                json_file = json.loads(data, strict=False)

                manifest_dict[item.split("_en")[0]] = json_file
            except JSONDecodeError:
                pass
            except ClientResponseError:
                logger.error(f"Failed to fetch manifest {item}")

        return manifest_dict