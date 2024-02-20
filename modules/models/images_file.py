from typing import List


def _use_image(image: str) -> bool:
    return (
        image.startswith("./img")
        or image.startswith("./pics")
        or image.startswith("./illum/IllUrk")
        or image.startswith("./mom-italia")
    ) and not image.startswith("./illum/IllUrk/thumbnails")


class ImagesFile:
    def __init__(self, path: str):
        self.path = path

    def list_images(self) -> List[str]:
        with open(self.path, "r", encoding="iso-8859-1") as file:
            return [
                "http://images.monasterium.net/" + line[2:].strip()
                if line.startswith("./")
                else line.strip()
                for line in file.readlines()
                if _use_image(line.strip())
            ]
