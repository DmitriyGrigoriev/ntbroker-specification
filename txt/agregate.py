"""
Скрипт агрегации кодов маркировки (КИЗ) в транспортные упаковки.

Читает коды индивидуальных упаковок (small box) и коды групповых упаковок (middle box),
формирует XML-файл агрегации для загрузки в систему маркировки "Честный знак".

Структура выходного XML:
    unit_pack
        Document
            organisation (информация об организации)
            pack_content[] (содержимое каждой групповой упаковки)
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from xml.dom import minidom
from xml.sax.saxutils import escape

BASE_DIR = Path(__file__).parent
MIDDLE_FILE = BASE_DIR / "middle box cherry pomegranate.txt"
SMALL_FILE = BASE_DIR / "small box cherry pomegranate.txt"

# ИНН организации
LP_TIN = "9726009063"


def read_lines(path):
    """
    Читает файл и возвращает список непустых строк.

    Args:
        path: Путь к файлу.

    Returns:
        Список строк без пробельных символов по краям.
    """
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def create_aggregation_xml(middle_boxes, small_boxes, lp_tin):
    """
    Создаёт XML-структуру агрегации КИЗ.

    Args:
        middle_boxes: Список кодов групповых упаковок (блоков).
        small_boxes: Список кодов индивидуальных упаковок (КИЗ).
        lp_tin: ИНН организации.

    Returns:
        Корневой элемент XML-дерева.

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
    index = 0
    for pack_code in middle_boxes:
        pack_content = ET.SubElement(document, "pack_content")

        pc = ET.SubElement(pack_content, "pack_code")
        pc.text = escape(pack_code)

        for _ in range(kis_per_block):
            if index >= len(small_boxes):
                break
            cis = ET.SubElement(pack_content, "cis")
            cis.text = f"<![CDATA[{small_boxes[index]}]]>"
            index += 1

    return root


def format_xml(root):
    """
    Форматирует XML-дерево в читаемый вид с отступами.

    Args:
        root: Корневой элемент XML-дерева.

    Returns:
        Отформатированный XML в виде байтов (UTF-8).
    """
    rough_string = ET.tostring(root, encoding="utf-8")
    parsed = minidom.parseString(rough_string)
    pretty_xml = parsed.toprettyxml(indent="    ", encoding="utf-8")

    # Исправление экранирования CDATA (ElementTree их экранирует)
    pretty_xml = pretty_xml.replace(
        b"&lt;![CDATA[", b"<![CDATA["
    ).replace(
        b"]]&gt;", b"]]>"
    )

    return pretty_xml


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

    Читает входные файлы, создаёт XML агрегации и сохраняет результат.
    """
    middle_boxes = read_lines(MIDDLE_FILE)
    small_boxes = read_lines(SMALL_FILE)

    print(f"Загружено блоков: {len(middle_boxes)}")
    print(f"Загружено КИЗ: {len(small_boxes)}")
    print(f"КИЗ на блок: {len(small_boxes) // len(middle_boxes)}")

    root = create_aggregation_xml(middle_boxes, small_boxes, LP_TIN)
    pretty_xml = format_xml(root)

    output_path = generate_output_filename()
    with open(output_path, "wb") as f:
        f.write(pretty_xml)

    print(f"XML успешно создан: {output_path}")


if __name__ == "__main__":
    main()
