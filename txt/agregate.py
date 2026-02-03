"""
Скрипт агрегации кодов маркировки (КИЗ) в транспортные упаковки.

Читает коды индивидуальных упаковок (small box) и коды групповых упаковок (middle box),
формирует XML-файл агрегации для загрузки в систему маркировки "Честный знак".

Структура выходного XML:
    unit_pack
        Document
            organisation (информация об организации)
            pack_content[] (содержимое каждой групповой упаковки)

Поддерживаемые форматы входных файлов:
    - TXT: каждая строка содержит один код маркировки
    - CSV: первый столбец содержит код маркировки (остальные столбцы игнорируются)

Примеры использования:

    1. Запуск без параметров (использует файлы по умолчанию):
        python agregate.py

    2. Указание файлов через короткие флаги:
        python agregate.py -m "Блок апельсиновая жвачка.tx" -s "Киз апельсиновая жвачка.tx"

    3. Указание файлов через длинные флаги:
        python agregate.py --middle-file "Блок апельсиновая жвачка.tx" --small-file "Киз апельсиновая жвачка.txt"

    4. Работа с CSV файлами:
        python agregate.py -m middle_codes.csv -s small_codes.csv

    5. Указание выходного файла:
        python agregate.py -m middle.txt -s small.txt -o result.xml

    6. Указание ИНН организации:
        python agregate.py -m middle.txt -s small.txt --inn 1234567890

    7. Полный набор параметров:
        python agregate.py -m middle.csv -s small.csv -o output.xml --inn 9876543210

    8. Просмотр справки:
        python agregate.py --help
"""

import argparse
import csv
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from xml.dom import minidom
from xml.sax.saxutils import escape

BASE_DIR = Path(__file__).parent
MIDDLE_FILE = BASE_DIR / "Блок апельсиновая жвачка.txt"
SMALL_FILE = BASE_DIR / "Киз апельсиновая жвачка.txt"

# ИНН организации
LP_TIN = "9726009063"


def clean_code(code: str) -> str:
    """
    Очищает код маркировки от управляющих символов.

    Удаляет невидимые управляющие ASCII символы (0x00-0x1F, 0x7F),
    такие как GS (Group Separator 0x1D), null bytes, и другие.
    Оставляет только печатные символы и обычные пробелы.

    Args:
        code: Код маркировки, возможно содержащий спецсимволы.

    Returns:
        Очищенный код без управляющих символов.

    Examples:
        >>> clean_code("0104...\\x1d8005...\\x1d933...")
        '0104...8005...933...'
    """
    # Удаляем все управляющие символы ASCII (0x00-0x1F), кроме пробелов (0x20)
    # И символ DEL (0x7F)
    return ''.join(char for char in code if ord(char) >= 0x20 and ord(char) != 0x7F)


def detect_file_format(path: Path) -> str:
    """
    Определяет формат файла по расширению.

    Args:
        path: Путь к файлу.

    Returns:
        Строка с форматом файла ('txt' или 'csv').

    Raises:
        ValueError: Если формат файла не поддерживается.
    """
    suffix = path.suffix.lower()
    if suffix == ".txt":
        return "txt"
    elif suffix == ".csv":
        return "csv"
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {suffix}. Используйте .txt или .csv")


def read_codes(path: Path, file_format: str = None) -> list[str]:
    """
    Читает коды маркировки из файла в зависимости от формата.

    Args:
        path: Путь к файлу.
        file_format: Формат файла ('txt' или 'csv'). Если None, определяется автоматически.

    Returns:
        Список кодов маркировки (непустые строки).

    Raises:
        FileNotFoundError: Если файл не найден.
        ValueError: Если формат файла не поддерживается.
    """
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    if file_format is None:
        file_format = detect_file_format(path)

    codes = []

    if file_format == "txt":
        # Чтение TXT: каждая строка = один код
        with open(path, "r", encoding="utf-8") as f:
            codes = [clean_code(line.strip()) for line in f if line.strip()]

    elif file_format == "csv":
        # Чтение CSV: первый столбец = код маркировки
        with open(path, "r", encoding="utf-8", newline="") as f:
            # Автоопределение разделителя
            sample = f.read(1024)
            f.seek(0)
            sniffer = csv.Sniffer()

            try:
                dialect = sniffer.sniff(sample, delimiters=",;\t")
                has_header = sniffer.has_header(sample)
            except csv.Error:
                # Если не удалось определить, используем запятую по умолчанию
                dialect = csv.excel
                has_header = False

            reader = csv.reader(f, dialect)

            # Пропускаем заголовок, если он есть
            if has_header:
                next(reader, None)

            for row in reader:
                if row and row[0].strip():
                    codes.append(clean_code(row[0].strip()))

    return codes


def read_lines(path):
    """
    Устаревшая функция для обратной совместимости.
    Используйте read_codes() для новых проектов.

    Args:
        path: Путь к файлу.

    Returns:
        Список строк без пробельных символов по краям.
    """
    return read_codes(Path(path))


def create_aggregation_xml(middle_boxes, small_boxes, lp_tin):
    """
    Создаёт XML-структуру агрегации КИЗ.

    Args:
        middle_boxes: Список кодов групповых упаковок (блоков).
        small_boxes: Список кодов индивидуальных упаковок (КИЗ).
        lp_tin: ИНН организации.

    Returns:
        Кортеж (корневой элемент XML-дерева, словарь плейсхолдер -> код КИЗ).

    Raises:
        ValueError: Если количество КИЗ не делится нацело на количество блоков.
    """
    if len(middle_boxes) == 0:
        raise ValueError("Файл с кодами блоков пуст")

    if len(small_boxes) % len(middle_boxes) != 0:
        raise ValueError(
            f"Количество КИЗ ({len(small_boxes)}) не делится нацело "
            f"на количество блоков ({len(middle_boxes)})"
        )

    kis_per_block = len(small_boxes) // len(middle_boxes)

    root = ET.Element("unit_pack")
    document = ET.SubElement(root, "Document")

    # Блок организации
    organisation = ET.SubElement(document, "organisation")
    id_info = ET.SubElement(organisation, "id_info")
    ET.SubElement(id_info, "LP_info", LP_TIN=lp_tin)

    # Содержимое упаковок
    # Используем плейсхолдеры вместо CDATA, чтобы ET не экранировал спецсимволы
    cdata_map = {}
    cis_index = 0
    for pack_index, pack_code in enumerate(middle_boxes):
        pack_content = ET.SubElement(document, "pack_content")

        pc = ET.SubElement(pack_content, "pack_code")
        pc.text = escape(pack_code[:25]) # Первые 25 символов

        for _ in range(kis_per_block):
            if cis_index >= len(small_boxes):
                break
            cis = ET.SubElement(pack_content, "cis")
            cis_placeholder = f"__CDATA_CIS_{cis_index}__"
            cis.text = cis_placeholder
            cdata_map[cis_placeholder] = small_boxes[cis_index][:21]  # Первые 21 символ
            cis_index += 1

    return root, cdata_map


def format_xml(root, cdata_map=None):
    """
    Форматирует XML-дерево в читаемый вид с отступами.

    Args:
        root: Корневой элемент XML-дерева.
        cdata_map: Словарь плейсхолдер -> оригинальный код КИЗ для CDATA-секций.

    Returns:
        Отформатированный XML в виде байтов (UTF-8).
    """
    rough_string = ET.tostring(root, encoding="utf-8")
    parsed = minidom.parseString(rough_string)
    pretty_xml = parsed.toprettyxml(indent="    ", encoding="utf-8")

    # Заменяем плейсхолдеры на CDATA-секции с оригинальными (неэкранированными) кодами
    if cdata_map:
        for placeholder, original_code in cdata_map.items():
            pretty_xml = pretty_xml.replace(
                placeholder.encode("utf-8"),
                f"<![CDATA[{original_code}]]>".encode("utf-8"),
            )

    return pretty_xml


def parse_args() -> argparse.Namespace:
    """
    Парсит аргументы командной строки.

    Returns:
        Namespace с параметрами: middle_file, small_file, output, inn.
    """
    parser = argparse.ArgumentParser(
        description="Агрегация кодов маркировки (КИЗ) в транспортные упаковки.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python agregate.py -m middle.txt -s small.txt
  python agregate.py -m middle.csv -s small.csv -o result.xml
  python agregate.py --middle-file middle.txt --small-file small.txt --inn 1234567890
  python agregate.py  (использует файлы по умолчанию)
        """
    )

    parser.add_argument(
        "-m", "--middle-file",
        type=Path,
        default=None,
        help=f"Путь к файлу с кодами middle box (по умолчанию: {MIDDLE_FILE.name})"
    )

    parser.add_argument(
        "-s", "--small-file",
        type=Path,
        default=None,
        help=f"Путь к файлу с кодами small box (по умолчанию: {SMALL_FILE.name})"
    )

    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help="Путь к выходному XML-файлу (по умолчанию: agregate_YYYYMMDD_HHMMSS.xml)"
    )

    parser.add_argument(
        "--inn",
        type=str,
        default=LP_TIN,
        help=f"ИНН организации (по умолчанию: {LP_TIN})"
    )

    return parser.parse_args()


def generate_output_filename():
    """
    Генерирует имя выходного файла с текущей датой и временем.

    Returns:
        Path объект с именем файла вида agregate_YYYYMMDD_HHMMSS.xml
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return BASE_DIR / f"agregate_{timestamp}.xml"


def main():
    """
    Основная функция скрипта.

    Читает входные файлы (поддержка txt и csv), создаёт XML агрегации и сохраняет результат.
    Может принимать параметры командной строки или использовать значения по умолчанию.
    """
    args = parse_args()

    # Определение входных файлов (используем переданные или дефолтные)
    middle_file = args.middle_file if args.middle_file else MIDDLE_FILE
    small_file = args.small_file if args.small_file else SMALL_FILE

    # Определение выходного файла
    output_path = args.output if args.output else generate_output_filename()

    # Чтение файлов с кодами (поддержка txt и csv)
    try:
        print(f"Чтение файла middle box: {middle_file}")
        middle_boxes = read_codes(middle_file)

        print(f"Чтение файла small box: {small_file}")
        small_boxes = read_codes(small_file)
    except (FileNotFoundError, ValueError) as e:
        print(f"Ошибка: {e}")
        return 1

    # Информация о загруженных данных
    print(f"\nЗагружено блоков: {len(middle_boxes)}")
    print(f"Загружено КИЗ: {len(small_boxes)}")
    if len(middle_boxes) > 0:
        print(f"КИЗ на блок: {len(small_boxes) // len(middle_boxes)}")
    print(f"ИНН организации: {args.inn}\n")

    # Создание XML
    try:
        root, cdata_map = create_aggregation_xml(middle_boxes, small_boxes, args.inn)
        pretty_xml = format_xml(root, cdata_map)

        with open(output_path, "wb") as f:
            f.write(pretty_xml)

        print(f"XML успешно создан: {output_path}")
        return 0
    except ValueError as e:
        print(f"Ошибка при создании XML: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
