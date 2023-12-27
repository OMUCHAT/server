import pathlib
from dataclasses import dataclass

from .utils.debug import DEBUG


@dataclass
class Directories:
    data: pathlib.Path
    assets: pathlib.Path
    plugins: pathlib.Path

    def mkdir(self):
        self.data.mkdir(parents=True, exist_ok=True)
        self.assets.mkdir(parents=True, exist_ok=True)
        self.plugins.mkdir(parents=True, exist_ok=True)

    def to_json(self):
        return {
            "data": str(self.data),
            "assets": str(self.assets),
            "plugins": str(self.plugins),
        }

    def __post_init__(self):
        self.data = pathlib.Path(self.data)
        self.assets = pathlib.Path(self.assets)
        self.plugins = pathlib.Path(self.plugins)

    def __str__(self):
        return f"Directories(data={self.data}, assets={self.assets}, plugins={self.plugins})"

    def __repr__(self):
        return str(self)


def get_directories():
    cwd = pathlib.Path.cwd()
    if DEBUG:
        return Directories(
            data=cwd / "data",
            assets=cwd / "assets",
            plugins=cwd / ".." / "plugins",
        )
    return Directories(
        data=cwd / "data",
        assets=cwd / "assets",
        plugins=cwd / "plugins",
    )
