# import packages
import requests
import zipfile
import io
import os

# download kaggle cleaned_merged_season. You have to inspect the element on https://www.kaggle.com/datasets/joebeachcapital/fantasy-football and get the current url to download the zip.
def download_kaggle(save_dir):
    res = requests.get(url="https://storage.googleapis.com/kaggle-data-sets/3677153/6380645/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20231013%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20231013T072643Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=c02c6551064353ca8f23eba63d40892318d1b025888e4637fda9ce0104d005471bfd857892395b5b7c49ed38f608ccfa88cf857923bc5293fb1da162130a05cff5000ee86fa076df0a914da97b8e63ce81977875c5490971e952608083ca9efab42074b4cbc916d847d94d0f0ec4eb609e71155e51a64ba6c4ebae940790f6908293893ef8503e351acfa57ea5e8c2a1210c836bda63ae0dca71f5d28af262091cfbd0860aa4e2acce1137f325d946a1dcad11dc2c804c7b29470636333088fe2528bb8f42e3499d5792bf2b677e44d3669bc3ca20105f119cc35669b5e57fef03a7b36b024cefa6fc432c500fc53ac1aeb362c86cefd8791d34ec25b4ab0bfc", stream = True)
    if not res.ok:
       raise ValueError("This url has grown stale and needs to be updated. We recommend downloading the set manually from https://www.kaggle.com/datasets/joebeachcapital/fantasy-football and unzipping in landing/temporal")
    z = zipfile.ZipFile(io.BytesIO(res.content))
    z.extractall(save_dir)

# Download football-data for game odds
def download_gameodds(save_dir):
    # Notice the season is the second to last path element
    res = requests.get(url='https://www.football-data.co.uk/mmz4281/2223/E0.csv')
    if not res.ok:
       raise ValueError("Something went wrong with the download. Please download manually from https://www.football-data.co.uk/")
    with open(os.path.join(save_dir, 'football-data_2223.csv'), 'wb') as f:
        f.write(res.content)

def download_metdata(save_dir):
    # For now we only download weather data from the 22/23 season. Each file has summary for one month.
    metdata_urls = {
        '01_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_67cbebee-317f-49a6-a08d-b9a2430578fb',
        '02_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_7da1a384-a514-429f-ba33-61221a81e67e',
        '03_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_ee5ac320-3c09-4651-99bf-14cf0b4b654b',
        '04_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_82c7d6dd-a066-46d0-9ef1-6c4563b24c0f',
        '05_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_24601479-e5fc-4b94-87bb-94cc18fec9ab',
        '06_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_51b3ec2d-158a-4cfb-bc04-82fffda4817f',
        '07_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_ad67d968-8428-4df0-9fb7-35e5ef26bd23',
        '08_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_c866003f-6825-4d3e-a3c6-61d8316342e5',
        '09_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_5823d7e7-fc36-4b5f-8e98-c3e7551b5694',
        '10_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_2d924ad8-8602-4e2f-a8ea-8d9bb606fe3a',
        '11_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_36a6b24f-8f96-4baa-97f0-96ed5c5924e6',
        '12_22': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_88f4a9ae-d472-4cf1-8708-2b8477bebef7',
        '01_23': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_17d9cb41-85cf-4e8c-ae9e-b4c4246726e9',
        '02_23': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_36631462-35a1-4464-a702-b2ce01146a81',
        '03_23': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_5efb0b1f-ca69-4d4a-8820-0c75d296fec2',
        '04_23': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_343be08e-9ecc-41d3-b69d-f5412c6e347e',
        '05_23': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_384ed440-3904-4a5b-a9a6-27146a6f42d5',
        '06_23': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_af32f7ac-c76b-4c77-80ab-920860930a4a',
        '07_23': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_3b3c71ec-ae81-4a1e-bb01-72e3bd9f957f',
        '08_23': 'https://digital.nmla.metoffice.gov.uk/download/file/IO_52504f7c-43e5-44d9-8e5a-4f03018a468c'
    }


    # download meteorlogical data
    for key in metdata_urls.keys():
        res = requests.get(url=metdata_urls[key], stream=True)
        if not res.ok:
            print("Couldn't download", key, res.status_code)
            continue
        with open(os.path.join(save_dir, f'Metoffice_{key}.pdf'), mode='wb') as f:
            f.write(res.content)
