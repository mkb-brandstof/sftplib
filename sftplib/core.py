"""Module containing core functionality for sftplib.
"""

from __future__ import annotations

from typing import List, BinaryIO, Union, Iterator, TextIO, Optional
from contextlib import contextmanager
import pathlib
import os
import io

from sftplib.connection import Connection


Base = pathlib.WindowsPath if os.name == "nt" else pathlib.PosixPath


class SFTPPath(Base):
    """Extension of pathlib.Path for SFTP file systems."""

    def __init__(
        self, *args: str, conn: Optional[Connection] = None, **credentials
    ) -> None:
        self.__conn = conn
        self.__credentials = credentials
        super()._from_parts(args)

    @property
    def conn(self) -> Connection:
        """Returns SFTP connection.

        Returns:
            Connection: SFTP connection.
        """
        if self.__conn is None:
            self.__connect()
        return self.__conn

    def __connect(self) -> None:
        """Private method used to initiate SFTP connection."""
        self.__conn = Connection(hostname=self.hostname, **self.__credentials)
        self.__conn.open()

    def close(self) -> None:
        """Close SSH and SFTP connection."""
        if self.__conn is not None:
            self.__conn.close()

    def __truediv__(self, key: str) -> SFTPPath:
        if not isinstance(key, str):
            raise TypeError(
                f"unsupported operand type(s) for /: 'SFTPPath' and '{type(key).__name__}'"
            )
        return self.__class__(
            super().__truediv__(key.lstrip("/")), conn=self.__conn, **self.__credentials
        )

    @property
    def hostname(self) -> str:
        """Returns SFTP hostname.

        Returns:
            str: SFTP hostname.
        """
        return super().__str__().split("/", 1)[0]

    @property
    def key(self) -> str:
        """Returns the current directory key.

        Returns:
            str: The directory key.
        """
        try:
            _key = super().__str__().split('/', 1)[1]
        except IndexError:
            _key = ""
        if self.suffix == "":
            _key += "/"
        return _key

    def __repr__(self):
        return f"SFTPPath({str(self)})"

    def __str__(self):
        return f"sftp://{super().__str__()}"

    @contextmanager
    def open(
        self, mode: str = "rb"
    ) -> Union[BinaryIO, TextIO]:  # pylint: disable=arguments-differ
        """Open the file under the current path to read or write.

        Args:
            mode (str, optional): Defaults to "rb".

        Yields:
            BinaryIO: The file.
        """
        assert mode in {"rb", "r"}
        if "r" in mode:
            file = io.BytesIO()
            self.conn.client.getfo(self.key, file)
            file.seek(0)
            if "b" in mode:
                yield file
            else:
                yield io.TextIOWrapper(file, encoding="utf-8")

    @property
    def parent(self) -> SFTPPath:
        """Returns the parent path.

        Returns:
            SFTPPath: The parent path.
        """
        return self.__class__(super().parent, conn=self.__conn)

    @property
    def parents(self) -> List[SFTPPath]:
        """Returns a list of all parent paths.

        Returns:
            List[SFTPPath]: All parent paths.
        """
        return [self.__class__(parent, conn=self.__conn) for parent in super().parents]

    def iterdir(self) -> Iterator[SFTPPath]:
        """Iterate over current directory.

        Yields:
            SFTPPath: Path in current directory.
        """
        for key in self.conn.client.listdir(self.key):
            yield self.__class__(self, key, conn=self.__conn)

    def rglob(self, pattern: str = None) -> Iterator[SFTPPath]:
        # TODO: implement rglob correctly
        return self.iterdir()

    def mkdir(self, *args, **kwargs) -> NotImplemented:
        """Does nothing for SFTPPath.
        Included for compatibility with pathlib.Path behaviour.
        """
        return NotImplemented

    def exists(self) -> bool:
        """Check if the current path exists.

        Returns:
            bool: True if path exists.
        """
        return self in {
            parent for path in self.parent.iterdir() for parent in path.parents
        }

    def is_file(self) -> bool:
        """Check if path is file.

        Returns:
            bool: True if path is file.
        """
        if self.suffix == "":
            return False
        return self in self.parent.iterdir()

    def unlink(self) -> None:
        """Delete the current file."""
        self.conn.client.remove(self.key)

    def rmdir(self, _: bool = False) -> None:
        """Delete the current directory.

        Raises:
            NotImplementedError
        """
        raise NotImplementedError("rmdir is not implemented for SFTPPath.")
