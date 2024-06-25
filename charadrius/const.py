from aspidoceleon.constants import Constants
import os


class Const(Constants):
    def __init__(self):
        """
        Custom properties for this class.
        """
        super().__init__()
        self.PROJECT_NAME = "charadrius"

        # I/O
        self.PROJECT_WORKDIR = f"{self.default_volume}/{self.PROJECT_NAME}"
        os.makedirs(self.PROJECT_WORKDIR, exist_ok=True)


if __name__ == "__main__":
    const = Const()
