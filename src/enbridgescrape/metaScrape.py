import requests

from .utils import metacodes, paths

meta_download_path = paths.downloads / "MetaData"
meta_download_path.mkdir(exist_ok=True, parents=True)


def metaDump():
    for pipe_code in metacodes:
        file_url = f"https://linkwc.enbridge.com/Pointdata/{pipe_code}AllPoints.csv"
        local_filename = meta_download_path / f"{pipe_code}AllPoints.csv"
        try:
            response = requests.get(file_url, stream=True)
            response.raise_for_status()  # Check for HTTP errors

            with open(local_filename, 'wb') as local_file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        local_file.write(chunk)
            # print(f"File '{local_filename}' downloaded successfully.")

        except requests.exceptions.RequestException as e:
            print(f"failed: metaDump {pipe_code} -  {e}")

    print(f"metaDump completed {'*'*10} {'-'*10}")
