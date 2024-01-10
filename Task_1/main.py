# Основной файл.

from yargy import Parser, rule, and_, or_
from yargy.interpretation import fact
from yargy.pipelines import morph_pipeline
from yargy.predicates import gram, is_capitalized, normalized, gte, lte
import gzip
from dataclasses import dataclass
from typing import Iterator
import requests
import validators
from tqdm import tqdm

# От данного метода по сути смысла нет, поскольку ДЗ выполнялись в IDE PyCharm,
# и нужный файл можно было просто напрямую "перекинуть" из файлового менеджера ОС в
# директорию проекта, но, хотелось банально это "пощупать", посмотреть как это работает
# (в целях саморазвития)


def DownloadFileFromGivenURL(url_link: str = None, needed_file: str = None) -> str:
    """
    Upload the file using the required link.
    :arg:
        url_link (HTTP-URL, String): URL-link from which you need to download the required file.
        needed_file (String): Name of the file to be retrieved from the link.
    :return:
        file (Desired file extension): File of the required format that needed to be obtained from the link.
        report (String): The info about the result of the operation.
    """

    response = requests.get(url_link)
    report = ""

    try:
        with open(needed_file, 'wb') as file:
            file.write(response.content)
            report = f"The File \"{needed_file}\" from the Link {url_link} " \
                     f"has been successfully downloaded to your working directory!"
    except FileNotFoundError:
        report = f"The File \"{needed_file}\" at the Specified Link {url_link} was not found!"
    finally:
        return report


@dataclass
class Text:
    person_name: str
    person_birth_date: str
    person_birth_place: str


def ReadAllTextFromGZIPFile(filename: str) -> Iterator[Text]:
    with gzip.open(filename, "rt", encoding="utf-8") as f:
        for line in f:
            yield Text(*line.strip().split("\t"))


if __name__ == '__main__':
    # ссылка для скачки файла с данными
    # https://github.com/sssaaammmuuurrraaaiii/Natural_Language_Processing/blob/master/Task_1/news.txt.gz

    URL = input("Enter the Link Where You Want to Download the File: ")
    file = URL.removesuffix('/').split('/')[-1]

    if validators.url(URL) is True:
        print(DownloadFileFromGivenURL(URL, file))

    texts = list(ReadAllTextFromGZIPFile("news.txt.gz"))

    # ------------------------------------------------------------------------------
    # ПРАВИЛА
    PERSON = fact("Person", ["name", "birth_date", "birth_place"])
    NAME = fact("Name", ["first", "last"])
    BIRTH_DATE = fact('Birth_date', ['day', 'month', 'year'])
    BIRTH_PLACE = fact('Birth_place', ['place'])
    DAY = and_(gte(1), lte(31))
    MONTH = morph_pipeline([
        'январь',
        'февраль',
        'март',
        'апрель',
        'май',
        'июнь',
        'июль',
        'август',
        'сентябрь',
        'октябрь',
        'ноябрь',
        'декабрь'
    ])
    YEAR = and_(gte(1), lte(2023))

    name = rule(
        gram("Name").interpretation(NAME.first.inflected()),
        gram("Surname").interpretation(NAME.last.inflected())
    ).interpretation(NAME)

    birth_date = rule(
        DAY.interpretation(BIRTH_DATE.day).optional(),
        MONTH.interpretation(BIRTH_DATE.month).optional(),
        normalized('в').optional(),
        YEAR.interpretation(BIRTH_DATE.year),
        normalized('год').optional()
    ).interpretation(BIRTH_DATE).optional()

    birth_place = rule(
        normalized('в'),
        is_capitalized().interpretation(BIRTH_PLACE.place)
    ).interpretation(BIRTH_PLACE).optional()

    person = rule(
        NAME.interpretation(PERSON.name),
        normalized('родился'),
        or_(
            BIRTH_DATE.interpretation(PERSON.birth_date),
            BIRTH_PLACE.interpretation(PERSON.birth_place)
        ),
        or_(
            BIRTH_DATE.interpretation(PERSON.birth_date),
            BIRTH_PLACE.interpretation(PERSON.birth_place)
        )
    ).interpretation(PERSON)

    parser = Parser(PERSON)

    for text in tqdm(texts, disable=False):
        for match in parser.findall(text.text):
            print(match.fact)
